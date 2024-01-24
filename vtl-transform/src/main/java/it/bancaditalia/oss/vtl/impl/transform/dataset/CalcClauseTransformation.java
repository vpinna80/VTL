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

import static it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata.INSTANCE;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.AnalyticTransformation;
import it.bancaditalia.oss.vtl.impl.transform.bool.ConditionalTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerFunction;

public class CalcClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(CalcClauseTransformation.class);

	public static class CalcClauseItem extends TransformationImpl
	{
		private static final long serialVersionUID = 1L;

		private final String name;
		private final Class<? extends ComponentRole> role;
		private final Transformation calcClause;

		public CalcClauseItem(Class<? extends ComponentRole> role, String name, Transformation calcClause)
		{
			this.name = Variable.normalizeAlias(name);
			this.calcClause = calcClause;
			this.role = role;
		}

		public String getName()
		{
			return name;
		}

		public Class<? extends ComponentRole> getRole()
		{
			return role;
		}

		@Override
		public String toString()
		{
			return (role != null ? role.getSimpleName().toLowerCase() + " " : "") + name + " := " + calcClause;
		}

		@Override
		public boolean isTerminal()
		{
			return calcClause.isTerminal();
		}

		public Set<LeafTransformation> getTerminals()
		{
			return calcClause.getTerminals();
		}

		@Override
		public ScalarValue<?, ?, ?, ?> eval(TransformationScheme scheme)
		{
			return (ScalarValue<?, ?, ?, ?>) calcClause.eval(scheme);
		}

		@Override
		public VTLValueMetadata computeMetadata(TransformationScheme scheme)
		{
			VTLValueMetadata metadata = calcClause.getMetadata(scheme);
			if (metadata instanceof DataSetMetadata && ((DataSetMetadata) metadata).getMeasures().size() != 1)
				throw new VTLInvalidParameterException(metadata, ScalarValueMetadata.class);
			else
				return metadata;
		}
		
		public boolean isAnalytic()
		{
			return isAnalytic1(calcClause);
		}

		private static boolean isAnalytic1(Transformation calcClause)
		{
			if (calcClause instanceof AnalyticTransformation)
				return true;
			else if (calcClause instanceof BinaryTransformation)
			{
				final BinaryTransformation binaryTransformation = (BinaryTransformation) calcClause;
				return isAnalytic1(binaryTransformation.getLeftOperand()) || isAnalytic1(binaryTransformation.getRightOperand());
			} 
			else if (calcClause instanceof UnaryTransformation)
				return isAnalytic1(((UnaryTransformation) calcClause).getOperand());
			else if (calcClause instanceof ConditionalTransformation)
				return isAnalytic1(((ConditionalTransformation) calcClause).getCondition())
						|| isAnalytic1(((ConditionalTransformation) calcClause).getThenExpr())
						|| isAnalytic1(((ConditionalTransformation) calcClause).getElseExpr());
			else
				return false;
		}
	}

	private final List<CalcClauseItem> calcClauses;

	public CalcClauseTransformation(List<CalcClauseItem> items)
	{
		this.calcClauses = items;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return calcClauses.stream().map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);
		DataSet operand = (DataSet) getThisValue(scheme);

		final Map<Boolean, List<CalcClauseItem>> partitionedClauses = calcClauses.stream()
			.collect(partitioningBy(CalcClauseItem::isAnalytic));
		final List<CalcClauseItem> nonAnalyticClauses = partitionedClauses.get(false);
		final List<CalcClauseItem> analyticClauses = partitionedClauses.get(true);
		
		DataSetMetadata nonAnalyticResultMetadata = new DataStructureBuilder(metadata)
				.removeComponents(analyticClauses.stream().map(CalcClauseItem::getName).collect(toSet()))
				.build();
		
		String lineageString = calcClauses.stream().map(CalcClauseItem::getName).collect(joining(", ", "calc ", ""));
		
		// preserve original dataset if no nonAnalyticsClauses are present
		DataSet nonAnalyticResult = nonAnalyticClauses.size() == 0
			? operand
			: operand.mapKeepingKeys(nonAnalyticResultMetadata, dp -> LineageNode.of(lineageString, dp.getLineage()), dp -> {
					DatapointScope dpSession = new DatapointScope(dp, nonAnalyticResultMetadata);
					
					// place calculated components (eventually overriding existing ones) 
					Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> calcValues = 
						nonAnalyticClauses.stream()
							.collect(toConcurrentMap(
								clause -> nonAnalyticResultMetadata.getComponent(clause.getName()).get(),
								clause -> clause.eval(dpSession))
							);
					
					HashMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(NonIdentifier.class));
					map.keySet().removeAll(calcValues.keySet());
					map.putAll(calcValues);
					return map;
				});

		// TODO: more efficient way to compute this instead of reduction by joining
		return analyticClauses.stream()
			.map(calcAndRename(metadata, scheme, dp -> LineageNode.of(lineageString, dp.getLineage())))
			.reduce(this::joinByIDs)
			.map(anResult -> joinByIDs(anResult, nonAnalyticResult))
			.orElse(nonAnalyticResult);
	}
	
	private Function<CalcClauseItem, DataSet> calcAndRename(DataSetMetadata resultStructure, TransformationScheme scheme, SerFunction<DataPoint, Lineage> lineage)
	{
		return clause -> {
			LOGGER.debug("Evaluating calc expression {}", clause.calcClause.toString());
			DataSet clauseValue = (DataSet) clause.calcClause.eval(scheme);
			DataStructureComponent<Measure, ?, ?> measure = clauseValue.getMetadata().getMeasures().iterator().next();
	
			String newName = coalesce(clause.getName(), measure.getName());
			DataStructureComponent<?, ?, ?> newComponent = resultStructure.getComponent(newName).get();
			
			DataSetMetadata newStructure = new DataStructureBuilder(clauseValue.getMetadata())
				.removeComponent(measure)
				.addComponent(newComponent)
				.build();

			LOGGER.trace("Creating component {} from expression {}", newComponent, clause.calcClause.toString());
			return clauseValue.mapKeepingKeys(newStructure, lineage, dp -> {
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> values = new HashMap<>(dp);
				values.remove(measure);
				values.put(newComponent, dp.get(measure));
				return values;
			});
		};
	}
	
	private DataSet joinByIDs(DataSet left, DataSet right)
	{
		return left.mappedJoin(left.getMetadata().joinForOperators(right.getMetadata()), right, 
				(dpl, dpr) -> dpl.combine(dpr, (dp1, dp2) -> LineageNode.of(((LineageNode) dp1.getLineage()).getTransformation(), dp2.getLineage())), false);
	}

	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata operand = getThisMetadata(scheme);

		if (!(operand instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(operand, DataSetMetadata.class);

		final DataSetMetadata metadata = (DataSetMetadata) operand;
		DataStructureBuilder builder = new DataStructureBuilder(metadata);

		for (CalcClauseItem item : calcClauses)
		{
			VTLValueMetadata itemMeta;
			try
			{
				itemMeta = item.getMetadata(scheme);
			}
			catch (VTLException e)
			{
				throw new VTLNestedException("In calc clause " + item.toString(), e);
			}
			
			ValueDomainSubset<?, ?> domain;

			// get the domain of the calculated component
			if (itemMeta instanceof UnknownValueMetadata)
				return INSTANCE;
			else if (itemMeta instanceof DataSetMetadata)
			{
				// calc item expression is a component, check for mono-measure dataset
				DataSetMetadata value = (DataSetMetadata) itemMeta;
				if (value.getMeasures().size() != 1)
					throw new VTLExpectedComponentException(Measure.class, value);

				domain = value.getMeasures().iterator().next().getDomain();
			} 
			else
				domain = ((ScalarValueMetadata<?, ?>) itemMeta).getDomain();

			Optional<DataStructureComponent<?, ?, ?>> maybePresent = metadata.getComponent(item.getName());
			
			if (maybePresent.isPresent())
			{
				// existing component
				DataStructureComponent<?, ? extends ValueDomainSubset<?, ?>, ? extends ValueDomain> definedComponent = maybePresent.get();

				// disallow override of ids
				if (definedComponent.is(Identifier.class))
					throw new VTLInvariantIdentifiersException("calc", singleton(definedComponent.asRole(Identifier.class)));
				else
				{
					if (!definedComponent.getDomain().isAssignableFrom(domain))
						throw new VTLIncompatibleTypesException("calc", definedComponent.getDomain(), domain);
					else if (item.getRole() != null && !definedComponent.is(item.getRole()))
					{
						// switch role (from a non-id to any)
						builder.removeComponent(definedComponent);
						DataStructureComponent<?, ?, ?> newComponent = DataStructureComponentImpl.of(item.getName(), item.getRole(), domain);
						builder.addComponent(newComponent);
					}
				}
			}
			else
			{
				// new component
				Class<? extends ComponentRole> newComponent = item.getRole() == null ? Measure.class : item.getRole();
				builder = builder.addComponent(DataStructureComponentImpl.of(item.getName(), newComponent, domain));
			}
		}

		return builder.build();
	}

	@Override
	public String toString()
	{
		return calcClauses.stream().map(Object::toString).collect(joining(", ", "calc ", ""));
	}
}
