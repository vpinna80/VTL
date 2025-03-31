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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata.INSTANCE;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
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
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.AnalyticTransformation;
import it.bancaditalia.oss.vtl.impl.transform.bool.ConditionalTransformation;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class CalcClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(CalcClauseTransformation.class);

	public static class CalcClauseItem extends TransformationImpl
	{
		private static final long serialVersionUID = 1L;

		private final VTLAlias alias;
		private final Class<? extends Component> role;
		private final Transformation calcClause;

		public CalcClauseItem(Class<? extends Component> role, VTLAlias alias, Transformation calcClause)
		{
			this.alias = requireNonNull(alias);
			this.calcClause = calcClause;
			this.role = role;
		}

		public VTLAlias getAlias()
		{
			return alias;
		}

		public Class<? extends Component> getRole()
		{
			return role;
		}

		@Override
		public String toString()
		{
			return (role != null ? role.getSimpleName().toLowerCase() + " " : "") + alias + " := " + calcClause;
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
			if (metadata.isDataSet() && ((DataSetMetadata) metadata).getMeasures().size() != 1)
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
				.removeComponents(analyticClauses.stream().map(CalcClauseItem::getAlias).collect(toSet()))
				.build();
		
		DataStructureComponent<Identifier, ?, ?> timeId = metadata.getIDs().stream()
					.map(c -> c.asRole(Identifier.class))
					.filter(c -> TIMEDS.isAssignableFrom(c.getVariable().getDomain()))
					.collect(collectingAndThen(toSet(), s -> s.isEmpty() || s.size() > 1 ? null : s.iterator().next()));
		
		MetadataRepository repo = scheme.getRepository();
		// preserve original dataset if no nonAnalyticsClauses are present
		DataSet nonAnalyticResult = nonAnalyticClauses.size() == 0
			? operand
			: operand.mapKeepingKeys(nonAnalyticResultMetadata, lineageEnricher(this), dp -> {
					DatapointScope dpSession = new DatapointScope(repo, dp, operand.getMetadata(), timeId);
					
					// place calculated components (eventually overriding existing ones) 
					Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> calcValues = 
						nonAnalyticClauses.stream()
							.collect(toConcurrentMap(
								clause -> nonAnalyticResultMetadata.getComponent(clause.getAlias()).get(),
								clause -> clause.eval(dpSession))
							);
					
					HashMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(NonIdentifier.class));
					map.keySet().removeAll(calcValues.keySet());
					map.putAll(calcValues);
					return map;
				});

		// TODO: more efficient way to compute this instead of reduction by joining
		return analyticClauses.stream()
			.map(calcAndRename(metadata, scheme, lineageEnricher(this)))
			.reduce(this::joinByIDs)
			.map(anResult -> joinByIDs(anResult, nonAnalyticResult))
			.orElse(nonAnalyticResult);
	}
	
	private Function<CalcClauseItem, DataSet> calcAndRename(DataSetMetadata resultStructure, TransformationScheme scheme, SerUnaryOperator<Lineage> lineage)
	{
		return clause -> {
			LOGGER.debug("Evaluating calc expression {}", clause.calcClause.toString());
			DataSet clauseValue = (DataSet) clause.calcClause.eval(scheme);
			DataStructureComponent<Measure, ?, ?> measure = clauseValue.getMetadata().getMeasures().iterator().next();
	
			VTLAlias newName = coalesce(clause.getAlias(), measure.getVariable().getAlias());
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
		SerBinaryOperator<Lineage> enricher = LineageNode.lineage2Enricher(this);
		return left.mappedJoin(left.getMetadata().joinForOperators(right.getMetadata()), right, 
				(dpl, dpr) -> dpl.combine(dpr, (dp1, dp2) -> enricher.apply(dp1.getLineage(), dp2.getLineage())), false);
	}

	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata operand = getThisMetadata(scheme);

		if (!(operand.isDataSet()))
			throw new VTLInvalidParameterException(operand, DataSetMetadata.class);

		final DataSetMetadata metadata = (DataSetMetadata) operand;
		DataStructureBuilder builder = new DataStructureBuilder(metadata);
		MetadataRepository repo = scheme.getRepository();

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
			else if (itemMeta.isDataSet())
			{
				// calc item expression is a component, check for mono-measure dataset
				DataSetMetadata value = (DataSetMetadata) itemMeta;
				domain = value.getSingleton(Measure.class).getVariable().getDomain();
			} 
			else
				domain = ((ScalarValueMetadata<?, ?>) itemMeta).getDomain();

			Optional<DataStructureComponent<?, ?, ?>> maybePresent = metadata.getComponent(item.getAlias());
			
			if (maybePresent.isPresent())
			{
				// existing component
				DataStructureComponent<?, ? extends ValueDomainSubset<?, ?>, ? extends ValueDomain> definedComponent = maybePresent.get();

				// disallow override of ids
				if (definedComponent.is(Identifier.class))
					throw new VTLInvariantIdentifiersException("calc", singleton(definedComponent.asRole(Identifier.class)));
				else
				{
					if (!definedComponent.getVariable().getDomain().isAssignableFrom(domain))
						throw new VTLIncompatibleTypesException("calc", definedComponent.getVariable().getDomain(), domain);
					else if (item.getRole() != null && !definedComponent.is(item.getRole()))
					{
						// switch role (from a non-id to any)
						builder.removeComponent(definedComponent);
						DataStructureComponent<?, ?, ?> newComponent = repo.createTempVariable(item.getAlias(), domain).as(item.getRole());
						builder.addComponent(newComponent);
					}
				}
			}
			else
			{
				// new component
				Class<? extends Component> newComponent = item.getRole() == null ? Measure.class : item.getRole();
				builder = builder.addComponent(repo.createTempVariable(item.getAlias(), domain).as(newComponent));
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
