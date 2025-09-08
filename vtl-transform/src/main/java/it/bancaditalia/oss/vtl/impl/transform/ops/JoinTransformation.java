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
package it.bancaditalia.oss.vtl.impl.transform.ops;

import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.CROSS_JOIN;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.FULL_JOIN;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.INNER_JOIN;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.LEFT_JOIN;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder.toDataStructure;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineage2Enricher;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineagesEnricher;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithKeys;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.applyIf;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static it.bancaditalia.oss.vtl.util.Utils.tryWith;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLAmbiguousComponentException;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleStructuresException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnaliasedExpressionException;
import it.bancaditalia.oss.vtl.exceptions.VTLUniqueAliasException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.scope.JoinApplyScope;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public class JoinTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(JoinTransformation.class);

	public enum JoinOperator
	{
		LEFT_JOIN, INNER_JOIN, FULL_JOIN, CROSS_JOIN
	}

	public static class JoinOperand implements Serializable
	{
		private static final long serialVersionUID = 1L;

		private final Transformation operand;
		private final VTLAlias id;

		public JoinOperand(Transformation operand, VTLAlias id)
		{
			this.operand = operand;
			this.id = id != null ? id : operand instanceof VarIDOperand ? VTLAliasImpl.of(((VarIDOperand) operand).getText()) : null;
		}

		public Transformation getOperand()
		{
			return operand;
		}

		public VTLAlias getId()
		{
			return id;
		}

		@Override
		public String toString()
		{
			return operand + (id != null ? " as " + id : "");
		}
	}

	private final JoinOperator operator;
	private final List<VTLAlias> usingNames;
	private final List<JoinOperand> operands;
	private final Transformation apply;
	private final Transformation keepOrDrop;
	private final Transformation rename;
	private final Transformation filter;
	private final Transformation calc;
	private final Transformation aggr;

	private transient JoinOperand refOperand;

	public JoinTransformation(JoinOperator operator, List<JoinOperand> operands, List<VTLAlias> using, Transformation filter, Transformation apply, Transformation calc, Transformation aggr,
			Transformation keepOrDrop, Transformation rename)
	{
		this.operator = operator;
		this.filter = filter;
		this.calc = calc;
		this.aggr = aggr;
		this.apply = apply;
		this.rename = rename;
		this.operands = unmodifiableList(coalesce(operands, emptyList()));
		this.usingNames = coalesce(using, emptyList());
		this.keepOrDrop = keepOrDrop;
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return operands.stream().map(JoinOperand::getOperand).map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	@SuppressWarnings("java:S3864")
	public DataSet eval(TransformationScheme scheme)
	{
		LOGGER.debug("Preparing renamed datasets for join");

		Map<JoinOperand, DataSet> values = operands.stream().collect(toMapWithValues(operand -> (DataSet) operand.getOperand().eval(scheme)));
		MetadataRepository repo = scheme.getRepository();

		DataSet result;
		DataSetStructure metadata = (DataSetStructure) getMetadata(scheme);

		List<DataSetStructure> structures = values.values().stream().map(DataSet::getMetadata).collect(toList());
		Set<DataSetComponent<?, ?, ?>> commonIDs = getCommonIDs(structures);
		Set<DataSetComponent<?, ?, ?>> usingComponents = usingNames.isEmpty() ? commonIDs : getUsingComponents(values.get(refOperand).getMetadata(), structures);

		if (operator == CROSS_JOIN)
			result = doCrossJoin(repo, values);
		else if (operator == FULL_JOIN)
			result = doFullJoin(repo, values);
		else if (commonIDs.equals(usingComponents))
			result = joinCaseA(repo, values);
		else if (commonIDs.containsAll(usingComponents))
			result = joinCaseB1(repo, values, usingComponents);
		else
			result = joinCaseB2(repo, values);

		if (filter != null)
			result = (DataSet) filter.eval(new ThisScope(scheme, result));

		if (apply != null)
			result = applyClause(metadata, scheme, result);
		else if (calc != null)
			result = (DataSet) calc.eval(new ThisScope(scheme, result));
		else if (aggr != null)
			result = (DataSet) aggr.eval(new ThisScope(scheme, result));
		
		if (keepOrDrop != null)
			result = (DataSet) keepOrDrop.eval(new ThisScope(scheme, result));
		
		if (rename != null)
			result = (DataSet) rename.eval(new ThisScope(scheme, result));

		Set<DataSetComponent<?, ?, ?>> remaining = new HashSet<>(result.getMetadata());
		remaining.removeAll(metadata);

		DataSet finalResult = result;
		return new StreamWrapperDataSet(metadata, () -> finalResult.stream().map(dp -> {
				DataPointBuilder builder = new DataPointBuilder(DONT_SYNC);
				for (Entry<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> entry: dp.entrySet())
				{
					DataSetComponent<?, ?, ?> comp = entry.getKey();
					VTLAlias alias = comp.getAlias();
					if (alias.isComposed())
						comp = comp.getRenamed(alias.split().getValue());
					builder.add(comp, entry.getValue());
				}

				return builder.build(dp.getLineage(), metadata);
			}));
	}

	private DataSet doFullJoin(MetadataRepository repo, Map<JoinOperand, DataSet> values)
	{
		// Find out which component must be renamed inside each dataset
		Map<JoinOperand, DataSet> datasets = renameBefore(repo, values);

		// Structure before applying any clause
		Map<JoinOperand, DataSetStructure> datasetsMeta = datasets.keySet().stream().collect(toMapWithValues(k -> datasets.get(k).getMetadata()));
		DataSetStructure structureBefore = virtualStructure(repo, datasetsMeta, null, false);
		// fictious datapoints filled with nulls for when datapoints from some datasets are missing
		Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> filler = structureBefore.stream()
				.filter(k -> k.is(NonIdentifier.class))
				.collect(toMapWithValues(k -> NullValue.instance(k.getDomain())));

		SerFunction<Collection<Lineage>, Lineage> enricher = lineagesEnricher(this);
		SerCollector<DataPoint, ?, DataPoint> unifier = SerCollector.of(
				() -> new SimpleEntry<>(new ConcurrentHashMap<>(filler), new ConcurrentLinkedQueue<Lineage>()),
				(entry, dp) -> { entry.getKey().putAll(dp); entry.getValue().add(dp.getLineage()); },
				(entryLeft, entryRight) -> { throw new UnsupportedOperationException(); },
				entry -> new DataPointBuilder(entry.getKey(), DONT_SYNC).build(enricher.apply(entry.getValue()), structureBefore),
				EnumSet.of(CONCURRENT, UNORDERED)
			);
		

		LOGGER.debug("Joining all datapoints");

		return new FunctionDataSet<>(structureBefore, dss -> {
			// TODO: Memory hungry!!! Find some way to stream instead of building this big
			// index collection
			LOGGER.debug("Indexing all datapoints");

			try (Stream<DataPoint> allDpsStream = dss.stream().map(DataSet::stream).collect(concatenating(false)))
			{
				return allDpsStream
					.collect(groupingByConcurrent(dp -> dp.getValues(Identifier.class), unifier))
					.values()
					.stream();
			}
		}, datasets.values());
	}

	private DataSet doCrossJoin(MetadataRepository repo, Map<JoinOperand, DataSet> values)
	{
		DataSet result = null;
		DataSetStructureBuilder joinedBuilder = new DataSetStructureBuilder();
		
		for (int i = 0; i < operands.size(); i++)
		{
			JoinOperand op = operands.get(i);
			DataSet toJoin = renameAll(repo, op, values.get(op));
			joinedBuilder = joinedBuilder.addComponents(toJoin.getMetadata());
			
			if (result == null)
				result = toJoin;
			else
				result = result.flatmapKeepingKeys(joinedBuilder.build(), identity(), dp -> {
					Map<DataSetComponent<NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> template = dp.getValues(NonIdentifier.class);
					return toJoin.stream()
						.map(dpj -> {
							Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(template);
							map.putAll(dpj);
							return map;
						});
				});
		}

		Map<VTLAlias, List<VTLAlias>> unaliasedNames = joinedBuilder.build().stream().map(c -> c.getAlias())
				.filter(name -> name.isComposed())
				.map(name -> name.split())
				.collect(groupingByConcurrent(e -> e.getValue(), mapping(e -> e.getKey(), toList())));
		
		DataSetStructureBuilder dealiased = new DataSetStructureBuilder(joinedBuilder.build());
		for (VTLAlias cName: unaliasedNames.keySet())
		{
			List<VTLAlias> sources = unaliasedNames.get(cName);
			if (sources.size() == 1)
			{
				DataSetComponent<?, ?, ?> comp = result.getComponent(cName.in(sources.get(0))).get();
				dealiased = dealiased.removeComponent(comp).addComponent(comp.getRenamed(cName));
			}
		}
		
		DataSet finalResult = result;
		DataSetStructure dealiasedStructure = dealiased.build();
		
		SerUnaryOperator<Lineage> enricher = lineageEnricher(this);
		return new StreamWrapperDataSet(dealiasedStructure, () -> finalResult.stream().map(dp ->  
			dp.entrySet().stream()
				.map(keepingValue(applyIf(
						comp -> !dealiasedStructure.contains(comp) && comp.getAlias().isComposed(), 
						comp -> comp.getRenamed(comp.getAlias().split().getValue())
				))).collect(toDataPoint(enricher.apply(dp.getLineage()), dealiasedStructure))
			));
	}

	private DataSet renameAll(MetadataRepository repo, JoinOperand op, DataSet toJoin)
	{
		DataSetStructure renamedStructure = toJoin.getMetadata().stream()
			.map(c -> c.getRenamed(c.getAlias().in(op.getId())))
			.collect(toDataStructure());
		
		SerUnaryOperator<Lineage> enricher = lineageEnricher(this);
		return new FunctionDataSet<>(renamedStructure, ds -> ds.stream()
			.map(dp -> new DataPointBuilder()
				.addAll(dp.entrySet().stream()
					.map(keepingValue(c -> c.getRenamed(c.getAlias().in(op.getId()))))
					.collect(entriesToMap())
				).build(enricher.apply(dp.getLineage()), renamedStructure)
			), toJoin);
	}

	private DataSet joinForB2(DataSet lDs, DataSet rDs, DataSetStructure resultMetadata)
	{
		Map<Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> index = rDs.stream().collect(toMapWithKeys(dp -> dp.getValuesByNames(usingNames)));
		Set<VTLAlias> lDsComponentNames = lDs.getMetadata().stream().map(DataSetComponent::getAlias).collect(toSet());
		DataSetStructure stepMetadata = rDs.getMetadata().stream().filter(component -> !lDsComponentNames.contains(component.getAlias())).collect(toDataStructure());
		
		SerBinaryOperator<Lineage> enricher = LineageNode.lineage2Enricher(this);
		DataSet stepResult = lDs.mapKeepingKeys(stepMetadata, identity(), dp -> {
			var key = dp.getValuesByNames(usingNames);
			if (index.containsKey(key))
				return dp.combine(index.get(key), enricher);
			else
				return new DataPointBuilder(dp).addAll(resultMetadata.stream().filter(c -> !lDsComponentNames.contains(c.getAlias())).collect(toMapWithValues(NullValue::instanceFrom)))
						.build(dp.getLineage(), resultMetadata);
		});
		
		switch (operator)
		{
			case INNER_JOIN: return stepResult.filter(dp -> index.containsKey(dp.getValuesByNames(usingNames)), identity());
			case LEFT_JOIN: return stepResult;
			default: throw new UnsupportedOperationException("Case B2 incompatible with " + operator.toString().toLowerCase());
		}
	}

	private DataSet joinCaseB2(MetadataRepository repo, final Map<JoinOperand, DataSet> inputDatasets)
	{
		Map<JoinOperand, DataSet> datasets = renameBefore(repo, inputDatasets);
		DataSet refDataset = datasets.get(refOperand);
		Map<JoinOperand, DataSet> datasetsWoutRef = datasets.entrySet().stream().filter(e -> !e.getKey().equals(refOperand)).collect(entriesToMap());
		Map<JoinOperand, DataSetStructure> datasetsMeta = datasets.entrySet().stream().map(e -> new SimpleEntry<>(e.getKey(), e.getValue().getMetadata())).collect(entriesToMap());
		DataSetStructure resultMetadata = virtualStructure(repo, datasetsMeta, refDataset.getMetadata(), false);
		if (operator == LEFT_JOIN || operator == INNER_JOIN)
		{
			return new FunctionDataSet<>(resultMetadata, DataSet::stream, datasetsWoutRef.values().stream().reduce(refDataset, (ds1, ds2) -> joinForB2(ds1, ds2, resultMetadata)));
		}
		else
		{
			throw new UnsupportedOperationException(operator.toString().toLowerCase());
		}
	}

	private DataSet joinCaseA(MetadataRepository repo, Map<JoinOperand, DataSet> values)
	{
		DataSet result;
		// Find out which component must be renamed inside each dataset
		Map<JoinOperand, DataSet> datasets = renameBefore(repo, values);

		// Case A: join all to reference ds
		LOGGER.debug("Collecting all identifiers");
		Map<DataSet, Set<DataSetComponent<Identifier, ?, ?>>> ids = datasets.entrySet().stream().filter(entryByKey(op -> op != refOperand)).map(Entry::getValue)
				.map(toEntryWithValue(DataSet::getMetadata)).map(keepingKey(DataSetStructure::getIDs)).collect(entriesToMap());

		// TODO: Memory hungry!!! Find some way to stream instead of building this big
		// index collection
		LOGGER.debug("Indexing all datapoints");
		Map<DataSet, ? extends Map<Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint>> indexes = datasets.entrySet().stream().filter(entryByKey(op -> op != refOperand))
				.map(Entry::getValue).collect(toMapWithValues(ds -> tryWith(ds::stream, stream -> {
					// toMap instead of groupingBy because there's never more than one datapoint in
					// each group
					return (LOGGER.isTraceEnabled() ? stream.peek(dp -> LOGGER.trace("Indexing {}", dp)) : stream).collect(toConcurrentMap(dp -> dp.getValues(Identifier.class), identity()));
				})));

		// Structure before applying any clause
		DataSetStructure structureBefore = datasets.values().stream().map(DataSet::getMetadata).flatMap(Set::stream).collect(toDataStructure());

		LOGGER.debug("Joining all datapoints");

		SerBinaryOperator<Lineage> enricher = lineage2Enricher(this);
		result = new FunctionDataSet<>(structureBefore, dataset -> dataset.stream().peek(refDP -> LOGGER.trace("Joining {}", refDP)).map(refDP -> {
			// Get all datapoints from other datasets (there is no more than 1 for each
			// dataset)
			List<DataPoint> otherDPs = datasets.entrySet().stream()
					.filter(entryByKey(op -> op != refOperand))
					.map(Entry::getValue)
					.map(ds -> indexes.get(ds).get(refDP.getValues(ids.get(ds), Identifier.class)))
					.filter(Objects::nonNull)
					.collect(toList());

			switch (operator)
			{
				case INNER_JOIN:
				case LEFT_JOIN:
					if (otherDPs.size() != indexes.size())
						if (operator == INNER_JOIN)
							return null;
						else
						{
							var dp = otherDPs.stream().reduce(refDP, (dp1, dp2) -> dp1.combine(dp2, enricher));
							return new DataPointBuilder(dp)
									.addAll(structureBefore.stream()
											.filter(Predicate.not(dp::containsKey))
											.collect(toMapWithValues(NullValue::instanceFrom)))
									.build(refDP.getLineage(), structureBefore);
						}
					else
					{
						// Join all datapoints
						DataPoint accDP = refDP;
						for (DataPoint otherDP : otherDPs)
							accDP = accDP.combine(otherDP, enricher);

						LOGGER.trace("Joined {}", accDP);
						return accDP;
					}
				default:
					throw new UnsupportedOperationException(operator + " not implemented");
			}
		}).filter(Objects::nonNull), datasets.get(refOperand));
		return result;
	}

	private DataSet joinCaseB1(MetadataRepository repo, Map<JoinOperand, DataSet> datasets, Set<DataSetComponent<?, ?, ?>> usingComponents)
	{
		// Find out which component must be renamed
		datasets = renameBefore(repo, datasets);
		Map<JoinOperand, DataSetStructure> structures = datasets.entrySet().stream().map(keepingKey(DataSet::getMetadata)).collect(entriesToMap());

		DataSetStructure virtualStructure = virtualStructure(repo, structures, datasets.get(refOperand).getMetadata(), true);
		if (!virtualStructure.containsAll(usingComponents))
		{
			Set<DataSetComponent<?, ?, ?>> missing = new HashSet<>(virtualStructure);
			missing.removeAll(usingComponents);
			throw new VTLMissingComponentsException(missing, virtualStructure);
		}

		// Case B1: join all to reference ds
		LOGGER.debug("Joining using {}", usingComponents);

		throw new UnsupportedOperationException();
	}

	private Map<JoinOperand, DataSet> renameBefore(MetadataRepository repo, Map<JoinOperand, DataSet> datasets)
	{
		ConcurrentMap<DataSetComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
		Set<DataSetComponent<?, ?, ?>> toBeRenamed = datasets.values().stream()
				.map(DataSet::getMetadata)
				.flatMap(d -> d.stream())
				.filter(c -> unique.putIfAbsent(c, TRUE) != null)
				.filter(c -> usingNames.isEmpty() ? c.is(NonIdentifier.class) : !usingNames.contains(c.getAlias()))
				.collect(toSet());

		return datasets.entrySet().stream().map(keepingKey((op, ds) -> {
			DataSetStructure oldStructure = ds.getMetadata();

			// find components that must be renamed and add 'alias#' in front of their name
			DataSetStructure newStructure = oldStructure.stream().map(c -> toBeRenamed.contains(c) ? c.getRenamed(c.getAlias().in(op.getId())) : c)
					.reduce(new DataSetStructureBuilder(), DataSetStructureBuilder::addComponent, DataSetStructureBuilder::merge).build();

			if (newStructure.equals(oldStructure))
			{
				// no need to change the operand, the structure is the same
				LOGGER.trace("Structure of dataset {} will be kept", oldStructure);
				return ds;
			}

			LOGGER.trace("Structure of dataset {} will be changed to {}", oldStructure, newStructure);

			// Create the dataset operand renaming components in all its datapoints
			return new NamedDataSet(op.getId(),
					new FunctionDataSet<>(newStructure, dataset -> dataset.stream().map(
							dp -> dp.entrySet().stream()
								.map(keepingValue(c -> toBeRenamed.contains(c) ? c.getRenamed(c.getAlias().in(op.getId())) : c))
								.collect(toDataPoint(dp.getLineage(), newStructure))),
							ds));
		})).collect(entriesToMap());
	}

	private DataSet applyClause(DataSetStructure metadata, TransformationScheme scheme, DataSet dataset)
	{
		if (apply == null)
			return dataset;

		Set<DataSetComponent<Measure, ?, ?>> applyComponents = dataset.getMetadata().getMeasures().stream()
				.map(c -> c.getAlias())
				.map(a -> a.isComposed() ? a.split().getValue() : a)
				.distinct()
				.map(alias -> {
					ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) apply.getMetadata(new JoinApplyScope(scheme, alias, dataset.getMetadata()))).getDomain();
					DataSetComponent<Measure, ?, ?> component = DataSetComponentImpl.of(alias, domain, Measure.class);
					return component;
				}).collect(toSet());

		DataSetStructure applyMetadata = dataset.getMetadata().stream().filter(c -> !c.is(Measure.class) || !c.getAlias().isComposed()).collect(toDataStructure(applyComponents));

		return dataset.mapKeepingKeys(applyMetadata, lineageEnricher(this),
				dp -> applyComponents.stream().collect(toMapWithValues(c -> (ScalarValue<?, ?, ?, ?>) apply.eval(new JoinApplyScope(scheme, c.getAlias(), dp)))));
	}

	public DataSetStructure computeMetadata(TransformationScheme scheme)
	{
		try
		{
			// check if expressions have aliases
			operands.stream().filter(o -> o.getId() == null).map(JoinOperand::getOperand).findAny().ifPresent(unaliased -> {
				throw new VTLUnaliasedExpressionException(unaliased);
			});
	
			// check for duplicate aliases
			operands.stream().map(JoinOperand::getId).collect(groupingByConcurrent(identity(), counting())).entrySet().stream()
					.filter(e -> e.getValue() > 1)
					.map(Entry::getKey)
					.findAny()
					.ifPresent(alias -> {
						throw new VTLUniqueAliasException(operator.toString().toLowerCase(), alias);
					});
	
			Map<JoinOperand, DataSetStructure> datasetsMeta = new HashMap<>();
			for (JoinOperand op: operands)
			{
				VTLValueMetadata opMeta = op.getOperand().getMetadata(scheme);
				if (opMeta instanceof DataSetStructure)
					datasetsMeta.put(op, (DataSetStructure) opMeta);
				else
					throw new InvalidParameterException("Scalar expressions cannot be used in " + operator.toString().toLowerCase() + ": " + op);
			}
	
			DataSetStructure result;
			if (operator == CROSS_JOIN || operator == FULL_JOIN)
			{
				result = virtualStructure(scheme.getRepository(), datasetsMeta, datasetsMeta.get(refOperand), false);
				LOGGER.info("{}ing {}", operator, operands.stream().collect(toConcurrentMap(JoinOperand::getId, datasetsMeta::get)));
			}
			else
			{
				Entry<JoinOperand, Boolean> caseAorB1 = isCaseAorB1(datasetsMeta);
				refOperand = caseAorB1.getKey();
				result = virtualStructure(scheme.getRepository(), datasetsMeta, datasetsMeta.get(refOperand), caseAorB1.getValue());
				LOGGER.info("Joining {} to ({}: {})", 
							operands.stream().filter(op -> op != refOperand).collect(toConcurrentMap(JoinOperand::getId, datasetsMeta::get)), 
							refOperand.getId(), datasetsMeta.get(refOperand));
			}
	
			// modify the result structure as needed
			if (filter != null)
				result = (DataSetStructure) filter.getMetadata(new ThisScope(scheme, result));
			if (apply != null)
			{
				DataSetStructure applyResult = result;
				Set<? extends DataSetComponent<Measure, ?, ?>> applyComponents = applyResult.getMeasures().stream()
						.map(c -> c.getAlias().getMemberAlias())
						.distinct()
						.map(name -> { 
							JoinApplyScope scope = new JoinApplyScope(scheme, name, applyResult);
							ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) apply.getMetadata(scope)).getDomain();
							DataSetComponent<Measure, ?, ?> measure = DataSetComponentImpl.of(name, domain, Measure.class);
							return measure;
						}).collect(toSet());
	
				result = applyResult.stream().filter(c -> !c.is(Measure.class) || !c.getAlias().isComposed())
						.reduce(new DataSetStructureBuilder(), DataSetStructureBuilder::addComponent, DataSetStructureBuilder::merge).addComponents(applyComponents).build();
			}
			else if (calc != null)
				result = (DataSetStructure) calc.getMetadata(new ThisScope(scheme, result));
			else if (aggr != null)
				result = (DataSetStructure) aggr.getMetadata(new ThisScope(scheme, result));
			if (keepOrDrop != null)
				result = (DataSetStructure) keepOrDrop.getMetadata(new ThisScope(scheme, result));
			if (rename != null)
				result = (DataSetStructure) rename.getMetadata(new ThisScope(scheme, result));
	
			// check if keep - drop - rename has made some components unambiguous
			Map<VTLAlias, Set<DataSetComponent<?, ?, ?>>> ambiguousComps = result.stream()
					.filter(c -> c.getAlias().isComposed())
					.map(Utils.toEntryWithKey(c -> c.getAlias().split().getValue()))
					.collect(groupingByConcurrent(Entry::getKey, mapping(Entry::getValue, toSet())));
			Entry<VTLAlias, Set<DataSetComponent<?, ?, ?>>> ambiguousComp = ambiguousComps.isEmpty() ? null : ambiguousComps.entrySet().iterator().next();
	
			if (ambiguousComp != null)
				throw new VTLAmbiguousComponentException(ambiguousComp.getKey(), ambiguousComp.getValue());
			
			return result;
		}
		catch (RuntimeException e)
		{
			throw new VTLNestedException("In expression " + this, e);
		}
	}

	private Entry<JoinOperand, Boolean> isCaseAorB1(Map<JoinOperand, DataSetStructure> datasetsMeta)
	{
		// Determine the superset of all identifiers
		Set<DataSetComponent<Identifier, ?, ?>> allIDs = datasetsMeta.values().stream().flatMap(ds -> ds.getIDs().stream()).collect(toSet());

		// Get the common ids
		Set<DataSetComponent<?, ?, ?>> commonIDs = getCommonIDs(datasetsMeta.values());

		// Find the reference dataset if there is one. It must contain the superset of ids
		Optional<JoinOperand> refDataSet = datasetsMeta.entrySet().stream().filter(entryByValue(ds -> ds.containsAll(allIDs))).map(Entry::getKey).findFirst();

		// All datasets have the same ids?
		boolean sameIDs = refDataSet.isPresent();
		DataSetStructure last = null;
		for (DataSetStructure ds : datasetsMeta.values())
		{
			if (last != null)
				sameIDs &= ds.getIDs().equals(last.getIDs());
			last = ds;
		}

		if (usingNames.isEmpty()) // Case A
			if (operator == INNER_JOIN && refDataSet.isEmpty())
				throw new VTLException(operator.toString().toLowerCase() + " requires one dataset to contain all the identifiers from all other datasets.");
			else if ((operator == LEFT_JOIN || operator == FULL_JOIN) && !sameIDs)
				throw new VTLIncompatibleStructuresException(operator.toString().toLowerCase() + " requires all the input datasets to have the same identifiers",
						datasetsMeta.values().stream().map(DataSetStructure::getIDs).collect(toList()));
			else if (operator == CROSS_JOIN)
				throw new UnsupportedOperationException(operator.toString().toLowerCase() + " not implemented");
			else
				return new SimpleEntry<>(operator == INNER_JOIN ? refDataSet.get() : operands.get(0), TRUE);
		else if (operator == INNER_JOIN) // case B1-B2
			if (refDataSet.isEmpty())
				throw new VTLException(operator.toString().toLowerCase() + " requires one dataset to contain all the identifiers from all other datasets.");
			else if (commonIDs.stream().map(DataSetComponent::getAlias).collect(toSet()).containsAll(usingNames)) // case B1
				return new SimpleEntry<>(refDataSet.get(), TRUE);
			else // case B2
				throw new UnsupportedOperationException("inner_join case B2 not implemented");
		else if (operator == LEFT_JOIN) // case B1-B2
			if (sameIDs && commonIDs.stream().map(DataSetComponent::getAlias).collect(toSet()).containsAll(usingNames)) // Case B1
				return new SimpleEntry<>(refDataSet.get(), TRUE);
			else // case B2
			{
				DataSetStructure leftmost = datasetsMeta.get(operands.get(0));
				List<DataSetStructure> nonRefStructures = datasetsMeta.values().stream().filter(not(leftmost::equals)).collect(toList());

				Set<DataSetComponent<?, ?, ?>> usingComponents = getUsingComponents(leftmost, datasetsMeta.values());

				boolean sameIDsWithoutRefDataset = true;
				last = null;
				for (DataSetStructure ds : nonRefStructures)
				{
					if (last != null)
						sameIDsWithoutRefDataset &= ds.getIDs().equals(last.getIDs());
					last = ds;
				}

				// All the input Data Sets, except the reference Data Set, have the same
				// Identifiers [Id1, … , Idn];
				if (!sameIDsWithoutRefDataset)
					throw new VTLIncompatibleStructuresException("left_join requires all the input datasets, except the leftmost one, to have the same identifiers",
							nonRefStructures.stream().map(DataSetStructure::getIDs).collect(toList()));

				// The using clause specifies all and only the common Identifiers of the
				// non-reference Data Sets [Id1, … , Idn].
				Set<DataSetComponent<?, ?, ?>> nonRefCommonIDs = getCommonIDs(nonRefStructures);
				if (!nonRefCommonIDs.stream().map(DataSetComponent::getAlias).collect(toSet()).equals(new HashSet<>(usingNames)))
					throw new VTLIncompatibleStructuresException("Error in the using clause", nonRefCommonIDs, usingComponents);

				return new SimpleEntry<>(operands.get(0), FALSE);
			}
		else
			throw new VTLException(operator.toString().toLowerCase() + " cannot have a using clause.");
	}

	private Set<DataSetComponent<?, ?, ?>> getUsingComponents(DataSetStructure refDataSet, Collection<DataSetStructure> datasets)
	{
		Map<VTLAlias, Boolean> unique = new ConcurrentHashMap<>();
		Set<DataSetComponent<?, ?, ?>> usingComps = datasets.stream()
				.flatMap(Collection::stream)
				.filter(c -> usingNames.contains(c.getAlias()))
				.filter(c -> unique.putIfAbsent(c.getAlias(), TRUE) == null)
				.map(c -> refDataSet.getComponent(c.getAlias()).orElse(c))
				.collect(toSet());

		Set<VTLAlias> missing = new HashSet<>(usingNames);
		for (DataSetComponent<?, ?, ?> c : usingComps)
			missing.remove(c.getAlias());

		if (missing.size() > 0)
		{
			unique.clear();
			Set<DataSetComponent<?, ?, ?>> available = datasets.stream().flatMap(Collection::stream).filter(c -> unique.putIfAbsent(c.getAlias(), TRUE) == null).collect(toSet());
			throw new VTLMissingComponentsException(available, missing.toArray(VTLAlias[]::new));
		}

		return usingComps;
	}

	private Set<DataSetComponent<?, ?, ?>> getCommonIDs(Collection<DataSetStructure> structures)
	{
		int howMany = structures.size();
		
		return structures.stream()
				.flatMap(ds -> ds.getIDs().stream())
				.collect(groupingByConcurrent(c -> c, counting())).entrySet().stream()
				.filter(entryByValue(c -> c.intValue() == howMany))
				.map(Entry::getKey)
				.collect(toSet());
	}

	@Override
	public boolean hasAnalytic()
	{
		return false;
	}

	private DataSetStructure virtualStructure(MetadataRepository repo, Map<JoinOperand, DataSetStructure> datasetsMeta, DataSetStructure refDataSet, boolean isCaseAorB1)
	{
		Set<DataSetComponent<?, ?, ?>> usingComponents = getUsingComponents(refDataSet, datasetsMeta.values());

		if (operator == CROSS_JOIN)
		{
			// rename all homonymous components
			ConcurrentMap<DataSetComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
			Set<DataSetComponent<?, ?, ?>> toBeRenamed = datasetsMeta.values().stream()
					.flatMap(Collection::stream)
					.filter(c -> unique.putIfAbsent(c, TRUE) != null)
					.collect(toSet());

			LOGGER.debug(operator.toString().toLowerCase() + " renames: {}", toBeRenamed);
			
			// Do the renaming
			DataSetStructureBuilder builder = new DataSetStructureBuilder();
			for (Entry<JoinOperand, DataSetStructure> e : datasetsMeta.entrySet())
				for (DataSetComponent<?, ?, ?> c : e.getValue())
					if (toBeRenamed.contains(c))
						builder.addComponent(c.getRenamed(c.getAlias().in(e.getKey().getId())));
					else
						builder.addComponent(c);

			return builder.build();
		}
		else if (operator == FULL_JOIN)
		{
			List<DataSetStructure> list = new ArrayList<>(datasetsMeta.values());
			DataSetStructure first = list.get(0);
			for (DataSetStructure current: list.subList(1, list.size()))
				if (!first.getIDs().equals(current.getIDs()))
					throw new VTLIncompatibleStructuresException("full_join", first.getIDs(), current.getIDs());
			
			// rename all homonymous non-id components
			Map<DataSetComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
			Set<DataSetComponent<?, ?, ?>> toBeRenamed = datasetsMeta.values()
					.stream()
					.flatMap(Collection::stream)
					.filter(c -> !c.is(Identifier.class))
					.filter(c -> unique.putIfAbsent(c, TRUE) != null)
					.collect(toSet());

			LOGGER.debug(operator.toString().toLowerCase() + " renames: {}", toBeRenamed);
			
			// Do the renaming
			DataSetStructureBuilder builder = new DataSetStructureBuilder();
			for (Entry<JoinOperand, DataSetStructure> e : datasetsMeta.entrySet())
				for (DataSetComponent<?, ?, ?> c : e.getValue())
					if (toBeRenamed.contains(c))
						builder.addComponent(c.getRenamed(c.getAlias().in(e.getKey().getId())));
					else
						builder.addComponent(c);

			return builder.build();
		}
		else if (isCaseAorB1)
		{
			// Case A: rename all measures and attributes with the same name
			// Case B1: rename all components with the same name except those 
			// in the using clause
			ConcurrentMap<DataSetComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
			Set<DataSetComponent<?, ?, ?>> toBeRenamed = datasetsMeta.values().stream()
					.flatMap(Collection::stream)
					.filter(c -> unique.putIfAbsent(c, TRUE) != null)
					.filter(c -> /* A */ usingComponents.isEmpty() && c.is(NonIdentifier.class) || /* B1 */ !usingComponents.isEmpty() && !usingComponents.contains(c))
					.collect(toSet());

			LOGGER.debug("Left/Inner join renames: {}", toBeRenamed);

			// Do the renaming
			DataSetStructureBuilder builder = new DataSetStructureBuilder();
			for (Entry<JoinOperand, DataSetStructure> e : datasetsMeta.entrySet())
				for (DataSetComponent<?, ?, ?> c : e.getValue())
					if (toBeRenamed.contains(c))
						builder.addComponent(c.getRenamed(c.getAlias().in(e.getKey().getId())));
					else
						builder.addComponent(c);

			return builder.build();
		}
		else
		{
			// case B2
			if (operator == INNER_JOIN)
				throw new UnsupportedOperationException("Inner join with using not implemented.");
			else if (operator == LEFT_JOIN)
			{
				// Components from using clause
				DataSetStructureBuilder builder = new DataSetStructureBuilder(usingComponents);
				// Other IDs of the ref data set
				builder.addComponents(refDataSet.getIDs());

				// Find unique components from all datasets
				Map<VTLAlias, Boolean> unique = new ConcurrentHashMap<>();
				Set<VTLAlias> toBeRenamed = datasetsMeta.values().stream()
						.flatMap(Collection::stream)
						.filter(c -> unique.putIfAbsent(c.getAlias(), TRUE) != null)
						.filter(c -> !usingNames.contains(c.getAlias()) && !(c.is(Identifier.class) && refDataSet.contains(c)))
						.map(DataSetComponent::getAlias)
						.collect(toSet());

				// add remaining components
				for (Entry<JoinOperand, DataSetStructure> e : datasetsMeta.entrySet())
					for (DataSetComponent<?, ?, ?> c : e.getValue())
						// rename non-unique components
						if (toBeRenamed.contains(c.getAlias()))
							builder.addComponent(c.getRenamed(c.getAlias().in(e.getKey().getId())));
						// add component from reference dataset if it has one, otherwise add it
						else if (!usingNames.contains(c.getAlias()))
							builder.addComponent(refDataSet.getComponent(c.getAlias()).orElse(c));

				return builder.build();
			}
			else
				throw new UnsupportedOperationException(operator.toString().toLowerCase());
		}

	}

	@Override
	public String toString()
	{
		return operator.toString().toLowerCase() + "(" + operands.stream().map(Object::toString).collect(joining(", "))
				+ (usingNames.isEmpty() ? "" : " using " + usingNames.stream().map(VTLAlias::toString).collect(joining(", "))) + (filter != null ? " " + filter : "") + (apply != null ? " apply " + apply : "")
				+ (calc != null ? " " + calc : "") + (aggr != null ? " " + aggr : "") + (keepOrDrop != null ? " " + keepOrDrop : "") + (rename != null ? " " + rename : "") + ")";
	}
}