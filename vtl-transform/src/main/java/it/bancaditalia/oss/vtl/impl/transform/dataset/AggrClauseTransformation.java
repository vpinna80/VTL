/**
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

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.AggregateTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

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
		private final Class<? extends Component> role;

		public AggrClauseItem(Class<? extends Component> role, String name, AggregateTransformation operand)
		{
			this.name = name;
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

		public Class<? extends Component> getRole()
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

		@Override
		public VTLValueMetadata getMetadata(TransformationScheme scheme)
		{
			return operand.getMetadata(scheme);
		}

		public AggrClauseItem withGroupBy(List<String> groupBy)
		{
			return new AggrClauseItem(role, name, new AggregateTransformation(operand, groupBy, name, role));
		}
	}

	private final List<AggrClauseItem> aggrItems;
	private final List<String> groupBy;
	private final Transformation having;

	private DataSetMetadata metadata;

	public AggrClauseTransformation(List<AggrClauseItem> operands, List<String> groupBy, Transformation having)
	{
		this.aggrItems = operands.stream().map(clause -> clause.withGroupBy(groupBy)).collect(toList());
		this.groupBy = groupBy == null || groupBy.isEmpty() ? null : groupBy;
		this.having = having;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return aggrItems.stream().map(AggrClauseItem::getOperand).map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet operand = (DataSet) getThisValue(session);
		
		TransformationScheme thisScope = new ThisScope(operand, session);
		
		List<DataSet> resultList = Utils.getStream(aggrItems)
			.map(item -> (DataSet) item.eval(thisScope))
			.collect(toList());
		
		DataSet result = resultList.get(0);
		DataSetMetadata currentStructure = result.getMetadata();
		for (int i = 1; i < resultList.size(); i++)
		{
			DataSet other = resultList.get(i);
			DataSetMetadata otherStructure = result.getMetadata();
			currentStructure = new DataStructureBuilder(currentStructure).addComponents(otherStructure).build();
			result = result.filteredMappedJoin(currentStructure, other, DataPoint::merge);
		}

		if (having != null)
			result = result.filter(dp -> (BooleanValue) having.eval(new DatapointScope(dp, metadata, session)) == BooleanValue.of(true));

		return result;
	}

	@Override
	public DataSetMetadata getMetadata(TransformationScheme session)
	{
		if (metadata != null)
			return metadata;

		VTLValueMetadata meta = getThisMetadata(session);

		if (meta instanceof DataSetMetadata)
		{
			DataSetMetadata operand = (DataSetMetadata) meta;

			Set<DataStructureComponent<Identifier, ?, ?>> identifiers = emptySet();
			if (groupBy != null)
			{
				Set<DataStructureComponent<?, ?, ?>> groupComps = groupBy.stream()
						.map(operand::getComponent)
						.map(o -> o.orElseThrow(() -> new VTLMissingComponentsException((DataSetMetadata) operand, groupBy.toArray(new String[0]))))
						.collect(toSet());
				
				Optional<DataStructureComponent<?, ?, ?>> nonID = groupComps.stream().filter(c -> c.is(NonIdentifier.class)).findAny();
				if (nonID.isPresent())
					throw new VTLIncompatibleRolesException("aggr with group by", nonID.get(), Identifier.class);
				
				identifiers = groupComps.stream().map(c -> c.as(Identifier.class)).collect(toSet());
			}

			DataStructureBuilder builder = new DataStructureBuilder().addComponents(identifiers);

			for (AggrClauseItem clause : aggrItems)
			{
				VTLValueMetadata clauseMeta = clause.getOperand().getMetadata(session);
				
				if (clauseMeta instanceof DataSetMetadata)
				{
					Set<DataStructureComponent<Measure, ?, ?>> measures = ((DataSetMetadata) clauseMeta).getComponents(Measure.class);
					if (measures.size() != 1)
						throw new VTLSingletonComponentRequiredException(Measure.class, measures);
					clauseMeta = (ScalarValueMetadata<?>) measures.iterator().next()::getDomain;
				}

				if (!(clauseMeta instanceof ScalarValueMetadata) || !Domains.NUMBERDS.isAssignableFrom(((ScalarValueMetadata<?>) clauseMeta).getDomain()))
					throw new VTLIncompatibleTypesException("Aggregation", Domains.NUMBERDS, ((ScalarValueMetadata<?>) clauseMeta).getDomain());

				Optional<DataStructureComponent<?,?,?>> maybeExistingComponent = operand.getComponent(clause.getComponent());
				Class<? extends Component> requestedRole = clause.getRole() == null ? Measure.class : clause.getRole();
				if (maybeExistingComponent.isPresent())
				{
					DataStructureComponent<?, ?, ?> existingComponent = maybeExistingComponent.get();
					if (existingComponent.is(Identifier.class))
						throw new VTLInvariantIdentifiersException("aggr", existingComponent.as(Identifier.class), requestedRole);
					else if (clause.getRole() == null)
						builder = builder.addComponent(existingComponent);
					else
						builder = builder.addComponent(new DataStructureComponentImpl<>(clause.getComponent(), requestedRole, Domains.NUMBERDS));
				}
				else
					builder = builder.addComponent(new DataStructureComponentImpl<>(clause.getComponent(), requestedRole, Domains.NUMBERDS));
			}

			metadata = builder.build();

			if (having != null)
			{
				throw new UnsupportedOperationException("HAVING not implemented.");
//				VTLValueMetadata vHaving = having.getMetadata(new ThisMetaEnvironment(metadata, session).getWrapperSession());
//				if (!(vHaving instanceof DataSetMetadata))
//					throw new VTLSyntaxException("Having clause must return a dataset.");
//
//				DataSetMetadata havingDS = (DataSetMetadata) vHaving;
//				if (havingDS.getComponents(Measure.class, Domains.BOOLEANDS).size() != 1)
//					throw new VTLExpectedComponentException(Measure.class, Domains.BOOLEANDS, havingDS.getComponents(Measure.class));
			}

			return metadata;
		}
		else
			throw new VTLInvalidParameterException(meta, DataSetMetadata.class);
	}

	@Override
	public String toString()
	{
		String terminator = (groupBy != null ? groupBy.stream().map(Object::toString).collect(joining(", ", " group by ", "")) : "") + (having != null ? " having " + having : "");
		return aggrItems.stream().map(Object::toString).collect(joining(", ", "[aggr ", terminator + "]"));
	}
}
