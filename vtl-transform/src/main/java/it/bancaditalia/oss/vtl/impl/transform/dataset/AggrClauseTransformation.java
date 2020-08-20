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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.ops.AggregateTransformation;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightF2DataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class AggrClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;

	private final static Logger LOGGER = LoggerFactory.getLogger(AggrClauseTransformation.class);

	public static class AggrClauseItem extends TransformationImpl
	{
		private static final long serialVersionUID = 1L;
		
		private final String                         component;
		private final AggregateTransformation        operand;
		private final Class<? extends ComponentRole> role;

		public AggrClauseItem(Class<? extends ComponentRole> role, String component, AggregateTransformation operand)
		{
			this.component = component;
			this.operand = operand;
			this.role = role;
		}

		public String getComponent()
		{
			return component;
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
			return (role != null ? role.getSimpleName().toUpperCase() + " " : "") + component + " := " + operand;
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
	}

	private final List<AggrClauseItem> operands;
	private final List<String>   groupBy;
	private final Transformation       having;

	private VTLDataSetMetadata metadata;

	public AggrClauseTransformation(List<AggrClauseItem> operands, List<String> groupBy, Transformation having)
	{
		this.operands = operands;
		this.groupBy = groupBy == null || groupBy.isEmpty() ? null : groupBy;
		this.having = having;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return operands.stream().map(AggrClauseItem::getOperand).map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet operand = (DataSet) getThisValue(session);

		BiFunction<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, Stream<DataPoint>, DataPoint> groupMapper = (keyValues, group) -> {
			LOGGER.trace("Aggregating group {}", keyValues);
			List<DataPoint> groupList = group.collect(toList());
			final DataPointBuilder groupResult = new DataPointBuilder(Utils.getStream(operands)
					.map(o -> {
						DataSet groupDataSet = new LightDataSet(operand.getDataStructure(), groupList::stream);
						DataStructureComponent<?, ?, ?> component = metadata.getComponent(o.getComponent()).get();
						ScalarValue<?, ?, ?> value = (ScalarValue<?, ?, ?>) o.getOperand().eval(new ThisScope(groupDataSet, session));

						LOGGER.trace("Computed {} as {}", component, value);

						return new SimpleEntry<>(component, value);
					}).collect(entriesToMap()));
			LOGGER.trace("Aggregated result {}", groupResult.addAll(keyValues));
			return groupResult.build(metadata);
		};

		return new LightF2DataSet<>(metadata, operand::streamByKeys, metadata.getComponents(Identifier.class), groupMapper);

		/*if (having != null)
		{
			DataSet dsHaving = (DataSet) having.eval(alias + "$having", new ThisValueEnvironment(result, session).getWrapperSession());
			DataStructureComponent<? extends Measure, BooleanDomainSubset, BooleanDomain> havingResult = dsHaving.getComponents(Measure.class, Domains.BOOLEANDS).iterator().next();

			result = result.filteredMappedJoin(alias, metadata, dsHaving, (a, b) -> ((BooleanValue) b.get(havingResult)).get(), (a, b) -> a);
		}*/

		// return result;
	}

	@Override
	public VTLDataSetMetadata getMetadata(TransformationScheme session)
	{
		if (metadata != null)
			return metadata;

		VTLValueMetadata meta = getThisMetadata(session);

		if (meta instanceof VTLDataSetMetadata)
		{
			VTLDataSetMetadata operand = (VTLDataSetMetadata) meta;

			Set<DataStructureComponent<Identifier, ?, ?>> identifiers = emptySet();
			if (groupBy != null)
			{
				Set<DataStructureComponent<?, ?, ?>> groupComps = groupBy.stream()
						.map(operand::getComponent)
						.map(o -> o.orElseThrow(() -> new VTLMissingComponentsException((DataStructure) operand, groupBy.toArray(new String[0]))))
						.collect(toSet());
				
				Optional<DataStructureComponent<?, ?, ?>> nonID = groupComps.stream().filter(c -> c.is(NonIdentifier.class)).findAny();
				if (nonID.isPresent())
					throw new VTLIncompatibleRolesException("aggr with group by", nonID.get(), Identifier.class);
				
				identifiers = groupComps.stream().map(c -> c.as(Identifier.class)).collect(toSet());
			}

			DataStructureBuilder builder = new DataStructureBuilder().addComponents(identifiers);

			for (AggrClauseItem clause : operands)
			{
				VTLValueMetadata clauseMeta = clause.getOperand().getMetadata(new DatapointScope(null, operand, session));
				if (!(clauseMeta instanceof VTLScalarValueMetadata) || !Domains.NUMBERDS.isAssignableFrom(((VTLScalarValueMetadata<?>) clauseMeta).getDomain()))
					throw new VTLIncompatibleTypesException("Aggregation", Domains.NUMBERDS, ((VTLScalarValueMetadata<?>) clauseMeta).getDomain());

				Optional<DataStructureComponent<?,?,?>> maybeExistingComponent = operand.getComponent(clause.getComponent());
				Class<? extends ComponentRole> requestedRole = clause.getRole() == null ? Measure.class : clause.getRole();
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
//				if (!(vHaving instanceof VTLDataSetMetadata))
//					throw new VTLSyntaxException("Having clause must return a dataset.");
//
//				VTLDataSetMetadata havingDS = (VTLDataSetMetadata) vHaving;
//				if (havingDS.getComponents(Measure.class, Domains.BOOLEANDS).size() != 1)
//					throw new VTLExpectedComponentException(Measure.class, Domains.BOOLEANDS, havingDS.getComponents(Measure.class));
			}

			return metadata;
		}
		else
			throw new VTLInvalidParameterException(meta, VTLDataSetMetadata.class);
	}

	@Override
	public String toString()
	{
		String terminator = (groupBy != null ? groupBy.stream().map(Object::toString).collect(joining(", ", " group by ", "")) : "") + (having != null ? " having " + having : "");
		return operands.stream().map(Object::toString).collect(joining(", ", "[aggr ", terminator + "]"));
	}
}
