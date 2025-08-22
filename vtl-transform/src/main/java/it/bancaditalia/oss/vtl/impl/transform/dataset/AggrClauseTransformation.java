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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineage2Enricher;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.AVG;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.COUNT;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.STDDEV_POP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.STDDEV_SAMP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.VAR_POP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.VAR_SAMP;
import static it.bancaditalia.oss.vtl.model.data.DataPoint.combinator;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleParametersException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.GroupingClauseImpl;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.AggregateTransformation;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;

public class AggrClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(AggrClauseTransformation.class);

	public static class AggrClauseItem implements Serializable
	{
		private static final long serialVersionUID = 1L;
		
		private final Class<? extends Component> role;
		private final VTLAlias component;
		private final AggregateTransformation operand;

		public AggrClauseItem(Class<? extends Component> role, VTLAlias component, AggregateTransformation operand)
		{
			this.role = role;
			this.component = component;
			this.operand = new AggregateTransformation(operand, component, coalesce(role, Measure.class));
		}

		public VTLAlias getName()
		{
			return component;
		}

		public AggregateTransformation getOperand()
		{
			return operand;
		}

		public Class<? extends Component> getRole()
		{
			return role;
		}

		@Override
		public String toString()
		{
			return (role != null ? role.getSimpleName().toUpperCase() + " " : "") + component + " := " + operand;
		}
	}

	private final List<AggrClauseItem> aggrItems;
	private final GroupingClauseImpl groupingClause;
	private final Transformation having;

	public AggrClauseTransformation(List<AggrClauseItem> aggrItems, GroupingClauseImpl groupingClause, Transformation having)
	{
		this.aggrItems = aggrItems;
		this.groupingClause = groupingClause;
		this.having = having;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return aggrItems.stream().map(AggrClauseItem::getOperand).map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) getThisValue(scheme);

		TransformationScheme thisScope = new ThisScope(scheme, dataset);
		
		List<DataSet> resultList = aggrItems.stream()
			.map(AggrClauseItem::getOperand)
			.map(thisScope::eval)
			.map(DataSet.class::cast)
			.collect(toList());
		
		DataSet result = resultList.get(0);
		
		DataSetStructure currentStructure = result.getMetadata();
		for (int i = 1; i < resultList.size(); i++)
		{
			SerBinaryOperator<Lineage> enricher = lineage2Enricher(aggrItems.get(i).getOperand());
			DataSet other = resultList.get(i);
			DataSetStructure otherStructure = other.getMetadata();
			currentStructure = new DataSetStructureBuilder(currentStructure).addComponents(otherStructure).build();
			result = result.mappedJoin(currentStructure, other, combinator(enricher));
		}

		if (having != null)
		{
			DataSet dsHaving = (DataSet) having.eval(new ThisScope(scheme, dataset));
			DataSetComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> condMeasure = dsHaving.getMetadata().getSingleton(Measure.class, BOOLEANDS);
			result = result.filteredMappedJoin(currentStructure, dsHaving, condMeasure, lineage2Enricher(having));
		}

		return result.enrichLineage(lineageEnricher(this));
	}

	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = getThisMetadata(scheme);

		if (meta.isDataSet())
		{
			DataSetStructure operand = (DataSetStructure) meta;
			Set<DataSetComponent<Identifier, ?, ?>> identifiers = emptySet();
			if (groupingClause != null)
				identifiers = groupingClause.getGroupingComponents(operand);

			DataSetStructureBuilder builder = new DataSetStructureBuilder(identifiers)
					.addComponents(operand.getComponents(ViralAttribute.class));

			for (AggrClauseItem clause : aggrItems)
			{
				VTLValueMetadata clauseMeta = clause.getOperand().getMetadata(scheme);
				
				if (clauseMeta.isDataSet())
				{
					Set<DataSetComponent<Measure, ?, ?>> measures = ((DataSetStructure) clauseMeta).getMeasures();
					if (measures.size() != 1)
						throw new VTLSingletonComponentRequiredException(Measure.class, measures);
					final DataSetComponent<Measure, ?, ?> measure = measures.iterator().next();
					clauseMeta = ScalarValueMetadata.of(measure.getDomain());
				}

				if (!(!clauseMeta.isDataSet()) || !NUMBERDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) clauseMeta).getDomain()))
					throw new VTLIncompatibleTypesException("Aggregation", NUMBERDS, ((ScalarValueMetadata<?, ?>) clauseMeta).getDomain());

				Optional<DataSetComponent<?,?,?>> maybeExistingComponent = operand.getComponent(clause.getName());
				Class<? extends Component> requestedRole = clause.getRole() == null ? Measure.class : clause.getRole();
				if (maybeExistingComponent.isPresent())
				{
					DataSetComponent<?, ?, ?> existingComponent = maybeExistingComponent.get();
					if (existingComponent.is(Identifier.class))
						throw new VTLInvariantIdentifiersException("aggr", existingComponent.asRole(Identifier.class), requestedRole);
					else if (clause.getRole() == null)
						builder = builder.addComponent(existingComponent);
					else
						builder = builder.addComponent(DataSetComponentImpl.of(clause.getName(), NUMBERDS, requestedRole));
				}
				else
				{
					ValueDomainSubset<?, ?> targetDomain;
					if (clause.getOperand().getAggregation() == COUNT)
						targetDomain = INTEGERDS; 
					else if (EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP).contains(clause.getOperand().getAggregation()))
						targetDomain = NUMBERDS; 
					else
					{
						VTLValueMetadata metadata = clause.getOperand().getMetadata(scheme);
						targetDomain = !metadata.isDataSet() 
								? ((ScalarValueMetadata<?, ?>) metadata).getDomain() 
								: ((DataSetStructure) metadata).getSingleton(Measure.class).getDomain();
					}
					
					builder = builder.addComponent(DataSetComponentImpl.of(clause.getName(), targetDomain, requestedRole));
				}
			}

			DataSetStructure structure = builder.build();
			
			if (having != null)
			{
				VTLValueMetadata havingMeta = having.getMetadata(new ThisScope(scheme, structure));
				ValueDomainSubset<?, ?> domain;
				if (!havingMeta.isDataSet())
					domain = ((ScalarValueMetadata<?, ?>) havingMeta).getDomain();
				else if (havingMeta.isDataSet())
					domain = ((DataSetStructure) havingMeta).getSingleton(Measure.class).getDomain();
				else
					domain = null;
					
				if (domain != null && !Domains.BOOLEANDS.isAssignableFrom(domain))
					throw new VTLIncompatibleParametersException("having", BOOLEAN, havingMeta);
			} 

			return structure;
		}
		else
			throw new VTLInvalidParameterException(meta, DataSetStructure.class);
	}

	@Override
	public String toString()
	{
		String terminator = (groupingClause != null ? " " + groupingClause.toString() : "") + (having != null ? " having " + having : "");
		return aggrItems.stream().map(Object::toString).collect(joining(", ", "aggr ", terminator));
	}
}
