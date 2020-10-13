/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.transform.aggregation;

import static it.bancaditalia.oss.vtl.impl.transform.aggregation.AnalyticTransformation.OrderingMethod.DESC;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.OffsetTransformation.OffsetDirection.LEAD;
import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toSet;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
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

public class OffsetTransformation extends UnaryTransformation implements AnalyticTransformation
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(OffsetTransformation.class);

	public enum OffsetDirection
	{
		LAG, LEAD
	}
	
	private final List<String> partitionBy;
	private final List<OrderByItem> orderByClause;
	private final OffsetDirection direction;
	private final int offset;
	private final ScalarValue<?,?,?> defaultValue;

	private transient DataSetMetadata metadata;

	public OffsetTransformation(OffsetDirection direction, Transformation operand, IntegerValue offset, ScalarValue<?, ?, ?> defaultValue, List<String> partitionBy, List<OrderByItem> orderByClause)
	{
		super(operand);

		this.direction = direction;
		this.offset = (int) (long) offset.get();
		this.defaultValue = defaultValue;
		this.partitionBy = coalesce(partitionBy, emptyList());
		this.orderByClause = coalesce(orderByClause, emptyList());
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
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

		// The ordering of the dataset
		final Comparator<DataPoint> comparator = comparator(ordering);
		
		// sort each partition with the comparator and then perform the analytic computation on each partition
		return new LightFDataSet<>((DataSetMetadata) metadata, ds -> ds.streamByKeys(
				partitionIDs, 
				toConcurrentMap(identity(), identity(), (a, b) -> a, () -> new ConcurrentSkipListMap<>(comparator)), 
				(partition, keyValues) -> offsetPartition(partition.keySet(), keyValues)
			).reduce(Stream::concat)
			.orElse(Stream.empty()), dataset);
	}
	
	private Stream<DataPoint> offsetPartition(NavigableSet<DataPoint> partition, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues)
	{
		LOGGER.debug("Analytic invocation on partition {}", keyValues);
		
		return Utils.getStream(partition)
				.map(dp -> {
					DataPoint offsetDatapoint = dp;
					UnaryOperator<DataPoint> op = direction == LEAD ? partition::lower : partition::higher;
					for (int i = 0; i < offset && offsetDatapoint != null; i++)
						offsetDatapoint = op.apply(offsetDatapoint);

					DataPointBuilder resultBuilder = new DataPointBuilder(dp.getValues(Identifier.class))
							.addAll(dp.getValues(Attribute.class));

					if (offsetDatapoint == null)
					{
						HashMap<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?>> nullContents = new HashMap<>(dp.getValues(Measure.class));
						nullContents.replaceAll((m, v) -> defaultValue == null ? NullValue.instanceFrom(m) : m.cast(defaultValue));
						resultBuilder = resultBuilder.addAll(nullContents);
					}
					else
						resultBuilder = resultBuilder.addAll(offsetDatapoint.getValues(Measure.class));

					return resultBuilder.build(metadata);
				});
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
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = session.getMetadata(THIS);
		if (opmeta instanceof ScalarValueMetadata)
			throw new VTLInvalidParameterException(opmeta, DataSetMetadata.class);
		
		metadata = (DataSetMetadata) opmeta;
		
		LinkedHashMap<DataStructureComponent<?, ?, ?>, Boolean> ordering = new LinkedHashMap<>();
		for (OrderByItem orderByComponent: orderByClause)
			ordering.put(metadata.getComponent(orderByComponent.getName()).get(), DESC != orderByComponent.getMethod());

		if (partitionBy != null)
			partitionBy.stream()
				.map(toEntryWithValue(metadata::getComponent))
				.map(e -> e.getValue().orElseThrow(() -> new VTLMissingComponentsException(e.getKey(), metadata)))
				.peek(c -> { if (!c.is(Identifier.class)) throw new VTLIncompatibleRolesException("partition by", c, Identifier.class); })
				.peek(c -> { if (ordering.containsKey(c)) throw new VTLException("Partitioning component " + c + " cannot be used in order by"); })
				.map(c -> c.as(Identifier.class))
				.collect(toSet());
		
		return metadata;
	}
	
	@Override
	public String toString()
	{
		return "rank(over (" 
				+ (partitionBy != null ? partitionBy.stream().collect(joining(", ", " partition by ", " ")) : "")
				+ (orderByClause != null ? orderByClause.stream().map(Object::toString).collect(joining(", ", " order by ", " ")) : "")
				+ ")";
	}
}
