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
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl.RANGE_UNBOUNDED_PRECEDING_TO_CURRENT;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.INT_VAR;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.impl.types.operators.AnalyticOperator.COUNT;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.DESC;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.util.SortClause;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.operators.AnalyticOperator;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion;

public class SimpleAnalyticTransformation extends UnaryTransformation implements AnalyticTransformation
{
	private static final long serialVersionUID = 1L;

	protected final AnalyticOperator aggregation;
	protected final List<VTLAlias> partitionBy;
	protected final List<OrderByItem> orderByClause;
	protected final WindowCriterion windowCriterion;

	public SimpleAnalyticTransformation(AnalyticOperator aggregation, Transformation operand, List<VTLAlias> partitionBy, 
				List<OrderByItem> orderByClause, WindowCriterion windowCriterion)
	{
		super(operand);
		
		this.aggregation = aggregation;
		this.partitionBy = coalesce(partitionBy, emptyList());
		this.orderByClause = coalesce(orderByClause, emptyList());
		this.windowCriterion = windowCriterion;
	}

	@Override
	protected VTLValue evalOnScalar(TransformationScheme scheme, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalOnDataset(TransformationScheme scheme, DataSet dataset, VTLValueMetadata metadata)
	{
		List<SortCriterion> ordering;
		if (orderByClause.isEmpty())
			ordering = dataset.getMetadata().getIDs().stream()
				.map(SortClause::new)
				.collect(toList());
		else
			ordering = orderByClause.stream()
				.map(toEntry(OrderByItem::getAlias, OrderByItem::getMethod))
				.map(keepingValue(dataset::getComponent))
				.map(keepingValue(Optional::get))
				.map(splitting(SortClause::new))
				.collect(toList());

		Set<DataSetComponent<Identifier, ?, ?>> partitionIDs = dataset.getMetadata().matchIdComponents(partitionBy, "partition by");
		partitionIDs.removeAll(ordering.stream().map(SortCriterion::getComponent).collect(toSet()));
		
		for (DataSetComponent<?, ?, ?> orderingComponent: ordering.stream().map(SortCriterion::getComponent).collect(toSet()))
			if (partitionIDs.contains(orderingComponent))
				throw new VTLException("Cannot order by " + orderingComponent.getAlias() + " because the component is used in partition by " + partitionBy);

		WindowCriterion criterion = coalesce(windowCriterion, orderByClause.isEmpty() 
				? RANGE_UNBOUNDED_PRECEDING_TO_CURRENT : DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING);
		WindowClause clause = new WindowClauseImpl(partitionIDs, ordering, criterion);
		
		for (DataSetComponent<Measure, ?, ?> measure: dataset.getMetadata().getMeasures())
			dataset = dataset.analytic(lineageEnricher(this), measure, measure, clause, 
					null, aggregation.getReducer(measure.getDomain()), null);

		return dataset;
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session);
		if (!opmeta.isDataSet())
			throw new VTLInvalidParameterException(opmeta, DataSetStructure.class);
		
		DataSetStructure dataset = (DataSetStructure) opmeta;
		
		LinkedHashMap<DataSetComponent<?, ?, ?>, Boolean> ordering = new LinkedHashMap<>();
		for (OrderByItem orderByComponent: orderByClause)
		{
			DataSetComponent<?, ?, ?> component = dataset.getComponent(orderByComponent.getAlias()).orElseThrow(() -> new VTLMissingComponentsException(dataset, orderByComponent.getAlias()));
			ordering.put(component, DESC != orderByComponent.getMethod());
		}

		Set<DataSetComponent<Identifier,?,?>> partitionComponents = dataset.matchIdComponents(partitionBy, "partition by");
		if (partitionBy.isEmpty())
			partitionComponents.removeAll(ordering.keySet());
		partitionComponents.retainAll(ordering.keySet());
		if (!partitionComponents.isEmpty())
			throw new VTLException("Partitioning components " + partitionComponents + " cannot be used in order by");

		DataSetStructureBuilder builder = new DataSetStructureBuilder(dataset);
		if (aggregation == COUNT)
			if (dataset.getMeasures().size() > 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, dataset.getMeasures());
			else
				builder = builder.removeComponent(dataset.getMeasures().iterator().next())
						.addComponent(INT_VAR);
			
		return builder.build();
	}
	
	@Override
	public String toString()
	{
		return aggregation + "(" + operand + " over (" 
				+ (partitionBy == null || partitionBy.isEmpty() ? "" : partitionBy.stream().map(VTLAlias::toString).collect(joining(", ", " partition by ", " ")))
				+ (orderByClause == null || orderByClause.isEmpty() ? "" : orderByClause.stream().map(Object::toString).collect(joining(", ", " order by ", " ")))
				+ windowCriterion + ")";
	}
}
