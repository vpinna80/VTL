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

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.DESC;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toCollection;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
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
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class RankTransformation extends TransformationImpl implements AnalyticTransformation, LeafTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain> RANK_MEASURE = INTEGERDS.getDefaultVariable().getComponent(Measure.class);
	private final static Logger LOGGER = LoggerFactory.getLogger(RankTransformation.class);

	private final List<String> partitionBy;
	private final List<OrderByItem> orderByClause;
	private final String lineageDescriptor;

	public RankTransformation(List<String> partitionBy, List<OrderByItem> orderByClause)
	{
		this.partitionBy = coalesce(partitionBy, emptyList()).stream().map(Variable::normalizeAlias).collect(toList());
		this.orderByClause = coalesce(orderByClause, emptyList());
		this.lineageDescriptor = "rank by " + orderByClause.stream().map(OrderByItem::getName).collect(joining(" "));
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) scheme.resolve(THIS); 
				
		Map<DataStructureComponent<?, ?, ?>, Boolean> ordering;
		
		if (orderByClause.isEmpty())
			ordering = dataset.getMetadata().getIDs().stream().collect(toMapWithValues(c -> TRUE));
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
				.map(c -> c.asRole(Identifier.class))
				.collect(toSet());
		else
			partitionIDs = dataset.getMetadata().getIDs().stream()
					.filter(partitionID -> !ordering.containsKey(partitionID))
					.collect(toSet());
		
		for (DataStructureComponent<?, ?, ?> orderingComponent: ordering.keySet())
			if (partitionIDs.contains(orderingComponent))
				throw new VTLException("Cannot order by " + orderingComponent.getVariable().getName() + " because the component is used in partition by " + partitionBy);

		// The ordering of the dataset
		final Comparator<DataPoint> comparator = comparator(ordering);
		
		// sort each partition with the comparator and then perform the analytic computation on each partition
		return new FunctionDataSet<>((DataSetMetadata) getMetadata(scheme), ds -> ds.streamByKeys(
				partitionIDs, 
				toCollection(() -> new ConcurrentSkipListSet<>(comparator)), 
				(partition, keyValues) -> rankPartition(scheme, partition, keyValues)
			).collect(concatenating(Utils.ORDERED)), dataset);
	}
	
	private Stream<DataPoint> rankPartition(TransformationScheme scheme, NavigableSet<DataPoint> partition, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		LOGGER.debug("Analytic invocation on partition {}", keyValues);
		long rank = 1, position = 1;
		Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> oldValues, measureValues = emptyMap();
		List<DataPoint> result = new ArrayList<>(partition.size());
		
		for (DataPoint dp: partition)
		{
			oldValues = measureValues;
			measureValues = dp.getValues(Measure.class);
			
			ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> rankResult;
			if (measureValues.equals(oldValues))
				rankResult = IntegerValue.of(rank);
			else
				// update rank if the new measures are different from the old
				rankResult = IntegerValue.of(rank = position);
			position++;
				
			result.add(new DataPointBuilder(dp.getValues(Identifier.class))
				.add(RANK_MEASURE, rankResult)
				.build(LineageNode.of(lineageDescriptor, dp.getLineage()), (DataSetMetadata) getMetadata(scheme)));
		}
		
		return result.stream();
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

	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata opmeta = scheme.getMetadata(THIS);
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
				.map(c -> c.asRole(Identifier.class))
				.collect(toSet());
		
		return new DataStructureBuilder(dataset.getIDs())
				.addComponent(RANK_MEASURE)
				.build();
	}
	
	@Override
	public String toString()
	{
		return "rank(over (" 
				+ (partitionBy != null ? partitionBy.stream().collect(joining(", ", " partition by ", " ")) : "")
				+ (orderByClause != null ? orderByClause.stream().map(Object::toString).collect(joining(", ", " order by ", " ")) : "")
				+ "))";
	}

	@Override
	public String getText()
	{
		return toString();
	}
}
