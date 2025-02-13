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
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl.DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.DESC;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.util.SortClause;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.operators.PartitionToRank;
import it.bancaditalia.oss.vtl.impl.types.window.RankedPartition;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class RankTransformation extends TransformationImpl implements AnalyticTransformation, LeafTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain> RANK_MEASURE = INTEGERDS.getDefaultVariable().as(Measure.class);
//	private final static Logger LOGGER = LoggerFactory.getLogger(RankTransformation.class);
	
	private final List<VTLAlias> partitionBy;
	private final List<OrderByItem> orderByClause;
	private final String lineageDescriptor;

	public RankTransformation(List<VTLAlias> partitionBy, List<OrderByItem> orderByClause)
	{
		this.partitionBy = coalesce(partitionBy, emptyList());
		this.orderByClause = coalesce(orderByClause, emptyList());
		this.lineageDescriptor = "rank by " + orderByClause.stream().map(OrderByItem::getAlias).map(VTLAlias::toString).collect(joining(" "));
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) scheme.resolve(THIS); 
		
		List<SortCriterion> ordering = new ArrayList<>(orderByClause.size()); 
		for (OrderByItem orderByComponent: orderByClause)
			ordering.add(new SortClause(dataset.getComponent(orderByComponent.getAlias()).get(), orderByComponent.getMethod()));

		Set<DataStructureComponent<Identifier, ?, ?>> partitionIDs;
		if (partitionBy != null)
			partitionIDs = partitionBy.stream()
				.map(dataset::getComponent)
				.map(Optional::get)
				.map(c -> c.asRole(Identifier.class))
				.collect(toSet());
		else
		{
			partitionIDs = new HashSet<>(dataset.getMetadata().getIDs());
			for (SortCriterion clause: ordering)
				partitionIDs.remove(clause.getComponent());
		}

		Set<VTLAlias> orderByAliases = orderByClause.stream().map(OrderByItem::getAlias).collect(toSet());
		
		WindowClause window = new WindowClauseImpl(partitionIDs, ordering, DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING);
		DataStructureComponent<Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		SerCollector<DataPoint, ?, RankedPartition> collector = 
				collectingAndThen(toList(PartitionToRank::new), l -> rankPartition(dataset.getMetadata(), orderByAliases, l));

		return dataset.analytic(lineage -> LineageNode.of(lineageDescriptor, lineage), measure, 
				INTEGERDS.getDefaultVariable().as(Measure.class), window, identity(), collector, (r, dp) -> finisher(r, dp))
			.membership(INTEGERDS.getDefaultVariable().getAlias());
	}
	
	private Collection<ScalarValue<?, ?, ?, ?>> finisher(RankedPartition ranks, DataPoint dp)
	{
		return singleton(IntegerValue.of(ranks.get(dp)));
	}

	private RankedPartition rankPartition(DataSetMetadata metadata, Set<VTLAlias> orderByAliases, PartitionToRank partition)
	{
		long rank = 1, position = 1;
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> oldValues, orderByValues = emptyMap();
		// Workaround to allow creating a spark encoder to correctly process the tuple
		RankedPartition ranks = new RankedPartition(partition.size());

		// Cannot use streams (perhaps gatherers when java version becomes high enough)
		for (DataPoint dp: partition)
		{
			oldValues = orderByValues;
			orderByValues = dp.getValuesByNames(orderByAliases);
			
			long rankResult;
			if (orderByValues.equals(oldValues))
				rankResult = rank;
			else
				// update rank if the new measures in the order by clause are different from the old
				rankResult = rank = position;
			position++;

			ranks.put(dp, rankResult);
		}
		
		return ranks;
	}

	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata opmeta = scheme.getMetadata(THIS);
		if (opmeta instanceof ScalarValueMetadata)
			throw new VTLInvalidParameterException(opmeta, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) opmeta;
		
		LinkedHashMap<DataStructureComponent<?, ?, ?>, Boolean> ordering = new LinkedHashMap<>();
		for (OrderByItem orderByComponent: orderByClause)
			ordering.put(dataset.getComponent(orderByComponent.getAlias()).get(), DESC != orderByComponent.getMethod());

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
				+ (partitionBy != null ? partitionBy.stream().map(VTLAlias::toString).collect(joining(", ", " partition by ", " ")) : "")
				+ (orderByClause != null ? orderByClause.stream().map(Object::toString).collect(joining(", ", " order by ", " ")) : "")
				+ "))";
	}

	@Override
	public String getText()
	{
		return toString();
	}
}
