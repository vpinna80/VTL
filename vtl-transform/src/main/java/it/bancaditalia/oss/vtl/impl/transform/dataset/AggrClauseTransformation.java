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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.normalizeAlias;
import static it.bancaditalia.oss.vtl.util.SerFunction.identity;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.GroupingClause;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.AggregateTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class AggrClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(AggrClauseTransformation.class);

	public static class AggrClauseItem extends TransformationImpl
	{
		private static final long serialVersionUID = 1L;
		
		private final String                         name;
		private final AggregateTransformation        operand;
		private final Class<? extends ComponentRole> role;

		public AggrClauseItem(Class<? extends ComponentRole> role, String name, AggregateTransformation operand)
		{
			this.name = normalizeAlias(name);
			this.operand = operand;
			this.role = role;
		}

		public String getComponent()
		{
			return name;
		}

		public AggregateTransformation getOperand()
		{
			return operand;
		}

		public Class<? extends ComponentRole> getRole()
		{
			return role;
		}

		@Override
		public String toString()
		{
			return (role != null ? role.getSimpleName().toUpperCase() + " " : "") + name + " := " + operand;
		}

		@Override
		public boolean isTerminal()
		{
			return false;
		}

		@Override
		public Set<LeafTransformation> getTerminals()
		{
			return operand.getTerminals();
		}

		@Override
		public VTLValue eval(TransformationScheme session)
		{
			return operand.eval(session);
		}

		public AggrClauseItem withGroupBy(GroupingClause groupingClause)
		{
			return new AggrClauseItem(role, name, new AggregateTransformation(operand, groupingClause, role, name));
		}

		@Override
		protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
		{
			return operand.getMetadata(scheme);
		}
	}

	private final List<AggrClauseItem> aggrItems;
	private final GroupingClause groupingClause;
	private final Transformation having;

	public AggrClauseTransformation(List<AggrClauseItem> aggrItems, GroupingClause groupingClause, Transformation having)
	{
		this.aggrItems = aggrItems.stream()
				.map(ac -> ac.withGroupBy(groupingClause))
				.collect(toList());
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
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);
		DataSet operand = (DataSet) getThisValue(scheme);

		TransformationScheme thisScope = new ThisScope(operand);
		
		List<DataSet> resultList = aggrItems.stream()
			.map(thisScope::eval)
			.map(DataSet.class::cast)
			.collect(toList());
		
		DataSet result = resultList.get(0);
		
		DataSetMetadata currentStructure = result.getMetadata();
		for (int i = 1; i < resultList.size(); i++)
		{
			AggrClauseItem aggrItem = aggrItems.get(i);
			DataSet other = resultList.get(i);
			DataSetMetadata otherStructure = other.getMetadata();
			currentStructure = new DataStructureBuilder(currentStructure).addComponents(otherStructure).build();
			result = result.mappedJoin(currentStructure, other, (dp1, dp2) -> {
				return dp1.combine(dp2, (d1, d2) -> LineageNode.of(aggrItem, dp1.getLineage(), dp2.getLineage()));
			}, false);
		}

		if (having != null)
			result = result.filter(dp -> (BooleanValue<?>) having.eval(new DatapointScope(dp, metadata)) == BooleanValue.of(true), lineage -> LineageNode.of(having, lineage));

		return result.mapKeepingKeys(metadata, dp -> LineageNode.of(this, dp.getLineage()), identity());
	}

	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = getThisMetadata(scheme);

		if (meta instanceof DataSetMetadata)
		{
			DataSetMetadata operand = (DataSetMetadata) meta;
			Set<DataStructureComponent<Identifier, ?, ?>> identifiers = emptySet();
			if (groupingClause != null)
				identifiers = groupingClause.getGroupingComponents(operand);

			DataStructureBuilder builder = new DataStructureBuilder().addComponents(identifiers);

			for (AggrClauseItem clause : aggrItems)
			{
				VTLValueMetadata clauseMeta = clause.getOperand().getMetadata(scheme);
				
				if (clauseMeta instanceof DataSetMetadata)
				{
					Set<DataStructureComponent<Measure, ?, ?>> measures = ((DataSetMetadata) clauseMeta).getMeasures();
					if (measures.size() != 1)
						throw new VTLSingletonComponentRequiredException(Measure.class, measures);
					final DataStructureComponent<Measure, ?, ?> measure = measures.iterator().next();
					clauseMeta = measure.getMetadata();
				}

				if (!(clauseMeta instanceof ScalarValueMetadata) || !NUMBERDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) clauseMeta).getDomain()))
					throw new VTLIncompatibleTypesException("Aggregation", NUMBERDS, ((ScalarValueMetadata<?, ?>) clauseMeta).getDomain());

				Optional<DataStructureComponent<?,?,?>> maybeExistingComponent = operand.getComponent(clause.getComponent());
				Class<? extends ComponentRole> requestedRole = clause.getRole() == null ? Measure.class : clause.getRole();
				if (maybeExistingComponent.isPresent())
				{
					DataStructureComponent<?, ?, ?> existingComponent = maybeExistingComponent.get();
					if (existingComponent.is(Identifier.class))
						throw new VTLInvariantIdentifiersException("aggr", existingComponent.asRole(Identifier.class), requestedRole);
					else if (clause.getRole() == null)
						builder = builder.addComponent(existingComponent);
					else
						builder = builder.addComponent(new DataStructureComponentImpl<>(clause.getComponent(), requestedRole, NUMBERDS));
				}
				else
					builder = builder.addComponent(new DataStructureComponentImpl<>(clause.getComponent(), requestedRole, NUMBERDS));
			}

			if (having != null)
			{
				throw new UnsupportedOperationException("HAVING not implemented.");
//				VTLValueMetadata vHaving = having.getMetadata(new ThisMetaEnvironment(metadata, session).getWrapperSession());
//				if (!(vHaving instanceof DataSetMetadata))
//					throw new VTLSyntaxException("Having clause must return a dataset.");
//
//				DataSetMetadata havingDS = (DataSetMetadata) vHaving;
//				if (havingDS.getComponents(Measure.class, Domains.BOOLEANDS).size() != 1)
//					throw new VTLExpectedComponentException(Measure.class, Domains.BOOLEANDS, havingDS.getMeasures());
			}

			return builder.build();
		}
		else
			throw new VTLInvalidParameterException(meta, DataSetMetadata.class);
	}

	@Override
	public String toString()
	{
		String terminator = (groupingClause != null ? " " + groupingClause.toString() : "") + (having != null ? " having " + having : "");
		return aggrItems.stream().map(Object::toString).collect(joining(", ", "aggr ", terminator));
	}
}
