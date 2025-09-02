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
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
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
import it.bancaditalia.oss.vtl.util.Utils;

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
		public boolean hasAnalytic()
		{
			return calcClause.hasAnalytic();
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
			if (metadata.isDataSet() && ((DataSetStructure) metadata).getMeasures().size() != 1)
				throw new VTLInvalidParameterException(metadata, ScalarValueMetadata.class);
			else
				return metadata;
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
		DataSetStructure destStructure = (DataSetStructure) getMetadata(scheme);
		DataSet operand = (DataSet) getThisValue(scheme);

		Map<Boolean, List<CalcClauseItem>> partitionedClauses = calcClauses.stream()
			.collect(partitioningBy(CalcClauseItem::hasAnalytic));
		List<CalcClauseItem> nonAnalyticClauses = partitionedClauses.get(false);
		List<CalcClauseItem> analyticClauses = partitionedClauses.get(true);
		
		DataSetStructure nonAnalyticResultMetadata = new DataSetStructureBuilder(destStructure)
				.removeComponents(analyticClauses.stream().map(CalcClauseItem::getAlias).collect(toSet()))
				.build();
		
		DataSetComponent<Identifier, ?, ?> timeId = destStructure.getIDs().stream()
					.map(c -> c.asRole(Identifier.class))
					.filter(c -> TIMEDS.isAssignableFrom(c.getDomain()))
					.collect(collectingAndThen(toSet(), s -> s.isEmpty() || s.size() > 1 ? null : s.iterator().next()));
		
		MetadataRepository repo = scheme.getRepository();
		// preserve original dataset if no nonAnalyticsClauses are present
		DataSetStructure opMeta = operand.getMetadata();
		DataSet nonAnalyticResult = nonAnalyticClauses.size() == 0
			? operand
			: operand.mapKeepingKeys(nonAnalyticResultMetadata, lineageEnricher(this), dp -> {
					DatapointScope dpSession = new DatapointScope(repo, dp, opMeta, timeId);

					Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> calcValues = new HashMap<>();
					for (CalcClauseItem clause: nonAnalyticClauses)
					{
						DataSetComponent<?, ?, ?> comp = nonAnalyticResultMetadata.getComponent(clause.getAlias()).get();
						ScalarValue<?, ?, ?, ?> value = clause.eval(dpSession);
						calcValues.put(comp, comp.getDomain().cast(value));
					}
					
					Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(NonIdentifier.class));
					map.keySet().removeAll(calcValues.keySet());
					map.putAll(calcValues);
					return map;
				});

		List<DataSet> analyticResults = new ArrayList<>();
		for (CalcClauseItem clause: analyticClauses)
		{
			LOGGER.debug("Evaluating calc expression {}", clause.calcClause.toString());
			DataSet clauseValue = (DataSet) clause.calcClause.eval(scheme);
			
			// srcMeasure is the single measure produced by the analytic invocation
			DataSetStructure clauseStructure = clauseValue.getMetadata();
			DataSetComponent<Measure, ?, ?> anMeasure = clauseStructure.getMeasures().iterator().next();

			// newComponent is the single component named in the calc clause to hold the result
			DataSetComponent<?, ?, ?> newComponent = destStructure.getComponent(clause.getAlias()).get();

			// put the analytic result into the prescribed component
			DataSetStructure newStructure = new DataSetStructureBuilder(clauseStructure.getIDs())
				.addComponent(newComponent)
				.build();
			
			DataSet analyticResult = clauseValue.mapKeepingKeys(newStructure, lineageEnricher(clause), 
				dp -> singletonMap(newComponent, dp.get(anMeasure)));

			analyticResults.add(analyticResult);
		}
		
		Optional<DataSet> anResult = Utils.getStream(analyticResults).reduce(this::joinByIDs);
		return anResult.isPresent() ? joinByIDs(nonAnalyticResult, anResult.get()) : nonAnalyticResult;
	}
	
	private DataSet joinByIDs(DataSet result, DataSet toJoin)
	{
		SerBinaryOperator<Lineage> enricher = LineageNode.lineage2Enricher(this);
		
		DataSetStructure joinedStructure = result.getMetadata().joinForOperators(toJoin.getMetadata());
		return result.mappedJoin(joinedStructure, toJoin, (dpl, dpr) -> dpl.combine(dpr, enricher));
	}

	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata operand = getThisMetadata(scheme);

		if (!(operand.isDataSet()))
			throw new VTLInvalidParameterException(operand, DataSetStructure.class);

		final DataSetStructure metadata = (DataSetStructure) operand;
		DataSetStructureBuilder builder = new DataSetStructureBuilder(metadata);

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
			
			ValueDomainSubset<?, ?> newDomain;

			// get the domain of the calculated component
			if (itemMeta instanceof UnknownValueMetadata)
				return INSTANCE;
			else if (itemMeta.isDataSet())
			{
				// calc item expression is a component, check for mono-measure dataset
				DataSetStructure value = (DataSetStructure) itemMeta;
				newDomain = value.getSingleton(Measure.class).getDomain();
			} 
			else
				newDomain = ((ScalarValueMetadata<?, ?>) itemMeta).getDomain();
			
			VTLAlias newAlias = item.getAlias();
			Class<? extends Component> newRole = Utils.coalesce(item.getRole(), Measure.class);
			DataSetComponent<? extends Component, ?, ?> newComp = DataSetComponentImpl.of(newAlias, newDomain, newRole);
			Optional<DataSetComponent<?, ?, ?>> maybePresent = metadata.getComponent(newAlias);
			
			if (maybePresent.isPresent())
			{
				// existing component
				DataSetComponent<?, ? extends ValueDomainSubset<?, ?>, ? extends ValueDomain> originalComp = maybePresent.get();

				// disallow override of ids
				if (originalComp.is(Identifier.class))
					throw new VTLInvariantIdentifiersException("calc", singleton(originalComp.asRole(Identifier.class)));
				else if (newAlias.equals(originalComp.getAlias()))
					builder = builder.removeComponent(originalComp);
			}

			builder = builder.addComponent(newComp);
		}

		return builder.build();
	}

	@Override
	public String toString()
	{
		return calcClauses.stream().map(Object::toString).collect(joining(", ", "calc ", ""));
	}
}
