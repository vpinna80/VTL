/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
package it.bancaditalia.oss.vtl.impl.transform.aggregation;

import static it.bancaditalia.oss.vtl.impl.transform.aggregation.AnalyticTransformation.OrderingMethod.DESC;
import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowView.UNBOUNDED_PRECEDING_TO_CURRENT_DATA_POINT;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerFunction.identity;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowView;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowView.WindowClause;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.types.operators.AnalyticOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class SimpleAnalyticTransformation extends UnaryTransformation implements AnalyticTransformation
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(SimpleAnalyticTransformation.class);

	private final AnalyticOperator	aggregation;
	private final List<String> partitionBy;
	private final List<OrderByItem> orderByClause;
	private final WindowClause windowClause;

	public SimpleAnalyticTransformation(AnalyticOperator aggregation, Transformation operand, List<String> partitionBy, List<OrderByItem> orderByClause, WindowClause windowClause)
	{
		super(operand);

		this.aggregation = aggregation;
		this.partitionBy = coalesce(partitionBy, emptyList());
		this.orderByClause = coalesce(orderByClause, emptyList());
		this.windowClause = coalesce(windowClause, UNBOUNDED_PRECEDING_TO_CURRENT_DATA_POINT);
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		Map<DataStructureComponent<?, ?, ?>, Boolean> ordering;
		
		if (orderByClause.isEmpty())
			ordering = dataset.getComponents(Identifier.class).stream().collect(Utils.toMapWithValues(c -> TRUE));
		else
		{
			ordering = new LinkedHashMap<>();
			for (OrderByItem orderByComponent: orderByClause)
				ordering.put(dataset.getComponent(orderByComponent.getName()).get(), DESC != orderByComponent.getMethod());
		}

		Set<DataStructureComponent<Identifier, ?, ?>> partitionIDs;
		if (partitionBy != null)
			partitionIDs = partitionBy.stream()
				.map(dataset::getComponent)
				.map(Optional::get)
				.map(c -> c.as(Identifier.class))
				.collect(toSet());
		else
			partitionIDs = Utils.getStream(dataset.getComponents(Identifier.class))
					.filter(partitionID -> !ordering.containsKey(partitionID))
					.collect(toSet());
		
		for (DataStructureComponent<?, ?, ?> orderingComponent: ordering.keySet())
			if (partitionIDs.contains(orderingComponent))
				throw new VTLException("Cannot order by " + orderingComponent.getName() + " because the component is used in partition by " + partitionBy);

		// The measures to aggregate
		Set<DataStructureComponent<Measure, ?, ?>> measures = dataset.getComponents(Measure.class);
		// The ordering of the dataset
		final Comparator<DataPoint> comparator = comparator(ordering);
		
		// sort each partition with the comparator and then perform the analytic computation on each partition
		return new LightFDataSet<>((DataSetMetadata) metadata, ds -> ds.streamByKeys(partitionIDs, toConcurrentMap(identity(), dp -> TRUE), 
				(partition, keyValues) -> aggregateWindows((DataSetMetadata) metadata, measures, comparator, partition.keySet(), keyValues)
			).collect(concatenating(Utils.ORDERED)), dataset);
	}
	
	private Stream<DataPoint> aggregateWindows(DataSetMetadata metadata, Set<DataStructureComponent<Measure, ?, ?>> measures, Comparator<DataPoint> comparator, Set<DataPoint> partition, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		LOGGER.debug("Analytic invocation on partition {}", keyValues);
		
		TreeSet<DataPoint> orderedPartition = new TreeSet<>(comparator);
		orderedPartition.addAll(partition);
		// for each window, compute aggregations for each measure and create a new datapoint with them
		return new WindowView(orderedPartition, windowClause)
				.getWindows()
				.map(window -> measures.stream()
					.map(toEntryWithValue(measure -> (ScalarValue<?, ?, ?, ?>) Utils.getStream(window.getValue()).collect(aggregation.getReducer(measure))))
					.collect(toDataPoint(getLineage(), metadata, window.getKey()))
				);
	}
	
	
	private static Comparator<DataPoint> comparator(Map<DataStructureComponent<?, ?, ?>, Boolean> sortMethods)
	{
		return (dp1, dp2) -> {
			for (Entry<DataStructureComponent<?, ?, ?>, Boolean> sortID: sortMethods.entrySet())
			{
				int res = dp1.get(sortID.getKey()).compareTo(dp2.get(sortID.getKey()));
				if (res != 0)
					return sortID.getValue() ? res : -res;
			}

			return 0;
		};
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session) ;
		if (opmeta instanceof ScalarValueMetadata)
			throw new VTLInvalidParameterException(opmeta, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) opmeta;
		
		LinkedHashMap<DataStructureComponent<?, ?, ?>, Boolean> ordering = new LinkedHashMap<>();
		for (OrderByItem orderByComponent: orderByClause)
			ordering.put(dataset.getComponent(orderByComponent.getName()).get(), DESC != orderByComponent.getMethod());

		if (partitionBy != null)
			partitionBy.stream()
				.map(toEntryWithValue(dataset::getComponent))
				.map(e -> e.getValue().orElseThrow(() -> new VTLMissingComponentsException(e.getKey(), dataset)))
				.peek(c -> { if (!c.is(Identifier.class)) throw new VTLIncompatibleRolesException("partition by", c, Identifier.class); })
				.peek(c -> { if (ordering.containsKey(c)) throw new VTLException("Partitioning component " + c + " cannot be used in order by"); })
				.map(c -> c.as(Identifier.class))
				.collect(toSet());
		
		return new DataStructureBuilder(dataset.getComponents(Identifier.class))
				.addComponents(dataset.getComponents(Measure.class))
				.build();
	}
	
	@Override
	public String toString()
	{
		return aggregation + "(" + operand + " over (" 
				+ (partitionBy == null || partitionBy.isEmpty() ? "" : partitionBy.stream().collect(joining(", ", " partition by ", " ")))
				+ (orderByClause == null || orderByClause.isEmpty() ? "" : orderByClause.stream().map(Object::toString).collect(joining(", ", " order by ", " ")))
				+ (windowClause == UNBOUNDED_PRECEDING_TO_CURRENT_DATA_POINT ? "" : windowClause)
				+ ")";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((aggregation == null) ? 0 : aggregation.hashCode());
		result = prime * result + ((orderByClause == null) ? 0 : orderByClause.hashCode());
		result = prime * result + ((partitionBy == null) ? 0 : partitionBy.hashCode());
		result = prime * result + ((windowClause == null) ? 0 : windowClause.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof SimpleAnalyticTransformation)) return false;
		SimpleAnalyticTransformation other = (SimpleAnalyticTransformation) obj;
		if (aggregation != other.aggregation) return false;
		if (orderByClause == null)
		{
			if (other.orderByClause != null) return false;
		}
		else if (!orderByClause.equals(other.orderByClause)) return false;
		if (partitionBy == null)
		{
			if (other.partitionBy != null) return false;
		}
		else if (!partitionBy.equals(other.partitionBy)) return false;
		if (windowClause == null)
		{
			if (other.windowClause != null) return false;
		}
		else if (!windowClause.equals(other.windowClause)) return false;
		return true;
	}
}
