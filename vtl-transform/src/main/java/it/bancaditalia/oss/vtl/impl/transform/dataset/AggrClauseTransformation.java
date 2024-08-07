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
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerFunction.identity;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.GroupingClause;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.AggregateTransformation;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class AggrClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(AggrClauseTransformation.class);

	public static class AggrClauseItem implements Serializable
	{
		private static final long serialVersionUID = 1L;
		
		private final String name;
		private final AggregateTransformation operand;
		private final Class<? extends Component> role;

		public AggrClauseItem(Class<? extends Component> role, String name, AggregateTransformation operand)
		{
			this.name = Variable.normalizeAlias(name);
			this.operand = operand;
			this.role = coalesce(role, Measure.class);
		}

		public AggrClauseItem(AggrClauseItem other, GroupingClause groupingClause)
		{
			this.name = other.name;
			this.role = other.role;
			this.operand = new AggregateTransformation(other.operand, groupingClause, role, name);
		}

		public String getComponent()
		{
			return name;
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
			return (role != null ? role.getSimpleName().toUpperCase() + " " : "") + name + " := " + operand;
		}
	}

	private final List<AggrClauseItem> aggrItems;
	private final GroupingClause groupingClause;
	private final Transformation having;

	public AggrClauseTransformation(List<AggrClauseItem> aggrItems, GroupingClause groupingClause, Transformation having)
	{
		this.aggrItems = aggrItems.stream()
				.map(item -> new AggrClauseItem(item, groupingClause))
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
		DataSet dataset = (DataSet) getThisValue(scheme);

		TransformationScheme thisScope = new ThisScope(scheme.getRepository(), dataset);
		
		List<DataSet> resultList = aggrItems.stream()
			.map(AggrClauseItem::getOperand)
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
				return dp1.combine(dp2, (d1, d2) -> LineageNode.of(aggrItem.getOperand(), dp1.getLineage(), dp2.getLineage()));
			}, false);
		}

		if (having != null)
			result = result.filter(dp -> (BooleanValue<?>) having.eval(new DatapointScope(scheme.getRepository(), dp, metadata, null)) == BooleanValue.of(true), lineage -> LineageNode.of(having, lineage));

		return result.mapKeepingKeys(metadata, dp -> LineageNode.of(this, dp.getLineage()), identity());
	}

	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = getThisMetadata(scheme);
		MetadataRepository repo = scheme.getRepository();

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
					clauseMeta = measure.getVariable();
				}

				if (!(clauseMeta instanceof ScalarValueMetadata) || !NUMBERDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) clauseMeta).getDomain()))
					throw new VTLIncompatibleTypesException("Aggregation", NUMBERDS, ((ScalarValueMetadata<?, ?>) clauseMeta).getDomain());

				Optional<DataStructureComponent<?,?,?>> maybeExistingComponent = operand.getComponent(clause.getComponent());
				Class<? extends Component> requestedRole = clause.getRole() == null ? Measure.class : clause.getRole();
				if (maybeExistingComponent.isPresent())
				{
					DataStructureComponent<?, ?, ?> existingComponent = maybeExistingComponent.get();
					if (existingComponent.is(Identifier.class))
						throw new VTLInvariantIdentifiersException("aggr", existingComponent.asRole(Identifier.class), requestedRole);
					else if (clause.getRole() == null)
						builder = builder.addComponent(existingComponent);
					else
						builder = builder.addComponent(repo.createTempVariable(clause.getComponent(), NUMBERDS).as(requestedRole));
				}
				else
					builder = builder.addComponent(repo.createTempVariable(clause.getComponent(), NUMBERDS).as(requestedRole));
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
