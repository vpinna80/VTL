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
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.DESC;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.util.SortClause;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.operators.AnalyticOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class SimpleAnalyticTransformation extends UnaryTransformation implements AnalyticTransformation
{
	private static final long serialVersionUID = 1L;

	protected final AnalyticOperator aggregation;
	protected final List<String> partitionBy;
	protected final List<OrderByItem> orderByClause;
	protected final WindowCriterion windowCriterion;

	public SimpleAnalyticTransformation(AnalyticOperator aggregation, Transformation operand, List<String> partitionBy, 
				List<OrderByItem> orderByClause, WindowCriterion windowCriterion)
	{
		super(operand);
		
		this.aggregation = aggregation;
		this.partitionBy = coalesce(partitionBy, emptyList()).stream().map(DataStructureComponent::normalizeAlias).collect(toList());
		this.orderByClause = coalesce(orderByClause, emptyList());
		this.windowCriterion = windowCriterion;
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		List<SortCriterion> ordering;
		if (orderByClause.isEmpty())
			ordering = dataset.getComponents(Identifier.class).stream()
				.map(c -> new SortClause(c, ASC))
				.collect(toList());
		else
			ordering = orderByClause.stream()
				.map(toEntry(OrderByItem::getName, OrderByItem::getMethod))
				.map(keepingValue(dataset::getComponent))
				.map(keepingValue(Optional::get))
				.map(splitting(SortClause::new))
				.collect(toList());

		Set<DataStructureComponent<Identifier, ?, ?>> partitionIDs = dataset.getMetadata().matchIdComponents(partitionBy, "partition by");
		partitionIDs.removeAll(ordering.stream().map(SortCriterion::getComponent).collect(toSet()));
		
		for (DataStructureComponent<?, ?, ?> orderingComponent: ordering.stream().map(SortCriterion::getComponent).collect(toSet()))
			if (partitionIDs.contains(orderingComponent))
				throw new VTLException("Cannot order by " + orderingComponent.getName() + " because the component is used in partition by " + partitionBy);

		WindowClause clause = new WindowClauseImpl(partitionIDs, ordering, windowCriterion);
		Set<DataStructureComponent<NonIdentifier, ?, ?>> nonIDs = dataset.getComponents(NonIdentifier.class);
		Map<DataStructureComponent<? extends NonIdentifier, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>>> collectors = nonIDs.stream()
			.collect(toMapWithValues(k -> aggregation.getReducer()));
		
		return dataset.analytic(dp -> LineageNode.of(this, dp.getLineage()), nonIDs, clause, collectors);
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session);
		if (opmeta instanceof ScalarValueMetadata)
			throw new VTLInvalidParameterException(opmeta, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) opmeta;
		
		LinkedHashMap<DataStructureComponent<?, ?, ?>, Boolean> ordering = new LinkedHashMap<>();
		for (OrderByItem orderByComponent: orderByClause)
		{
			DataStructureComponent<?, ?, ?> component = dataset.getComponent(orderByComponent.getName()).orElseThrow(() -> new VTLMissingComponentsException(orderByComponent.getName(), dataset));
			ordering.put(component, DESC != orderByComponent.getMethod());
		}

		Set<DataStructureComponent<Identifier,?,?>> partitionComponents = dataset.matchIdComponents(partitionBy, "partition by");
		partitionComponents.retainAll(ordering.keySet());
		if (!partitionComponents.isEmpty())
			throw new VTLException("Partitioning components " + partitionComponents + " cannot be used in order by");
		
		return dataset;
	}
	
	@Override
	public String toString()
	{
		return aggregation + "(" + operand + " over (" 
				+ (partitionBy == null || partitionBy.isEmpty() ? "" : partitionBy.stream().collect(joining(", ", " partition by ", " ")))
				+ (orderByClause == null || orderByClause.isEmpty() ? "" : orderByClause.stream().map(Object::toString).collect(joining(", ", " order by ", " ")))
				+ windowCriterion + ")";
	}
}
