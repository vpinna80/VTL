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

import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinCase.CASE_A;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinCase.CASE_B1;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinCase.CASE_B2;
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
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithKeys;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.applyIf;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithKey;
import static it.bancaditalia.oss.vtl.util.Utils.tryWith;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
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
import java.util.stream.IntStream;
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
import it.bancaditalia.oss.vtl.impl.types.names.MembershipAlias;
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
import it.bancaditalia.oss.vtl.util.SerCollectors;
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
	
	public enum JoinCase
	{
		CASE_A, CASE_B1, CASE_B2
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

		ConcurrentMap<JoinOperand, DataSetStructure> operandsStructures = values.entrySet().stream().map(Utils.keepingKey(DataSet::getMetadata)).collect(SerCollectors.entriesToMap());
		Set<DataSetComponent<?, ?, ?>> commonIDs = getCommonIDs(operandsStructures.values());
		Entry<JoinOperand, JoinCase> joinCase = tryJoinCases(operandsStructures);
		DataSet refDataSet = values.get(joinCase.getKey());
		Set<DataSetComponent<?, ?, ?>> usingComponents = usingNames.isEmpty() ? commonIDs : getUsingComponents(refDataSet.getMetadata(), operandsStructures.values());

		if (operator == CROSS_JOIN)
			result = doCrossJoin(repo, values);
		else if (operator == FULL_JOIN)
			result = doFullJoin(repo, values);
		else if (commonIDs.equals(usingComponents))
			result = joinCaseA(repo, values, joinCase.getKey());
		else if (commonIDs.containsAll(usingComponents))
			result = joinCaseB1(repo, values, usingComponents, joinCase.getKey());
		else
			result = joinCaseB2(repo, values, joinCase.getKey());

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
		DataSetStructure structureBefore = virtualStructure(repo, datasetsMeta, null, CASE_A);

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

	private DataSet joinCaseB2(MetadataRepository repo, Map<JoinOperand, DataSet> inputDatasets, JoinOperand refOperand)
	{
		Map<JoinOperand, DataSet> datasets = renameBefore(repo, inputDatasets);
		DataSet refDataset = datasets.get(refOperand);
		Map<JoinOperand, DataSet> datasetsWoutRef = datasets.entrySet().stream().filter(e -> !e.getKey().equals(refOperand)).collect(entriesToMap());
		Map<JoinOperand, DataSetStructure> datasetsMeta = datasets.entrySet().stream().map(e -> new SimpleEntry<>(e.getKey(), e.getValue().getMetadata())).collect(entriesToMap());
		DataSetStructure resultMetadata = virtualStructure(repo, datasetsMeta, refDataset.getMetadata(), CASE_B2);
		
		return new FunctionDataSet<>(resultMetadata, DataSet::stream, datasetsWoutRef.values().stream().reduce(refDataset, (ds1, ds2) -> joinForB2(ds1, ds2, resultMetadata)));
	}

	private DataSet joinCaseA(MetadataRepository repo, Map<JoinOperand, DataSet> values, JoinOperand refOperand)
	{
		DataSet result;
		// Find out which component must be renamed inside each dataset
		Map<JoinOperand, DataSet> datasets = renameBefore(repo, values);

		// Case A: join all to reference ds
		LOGGER.trace("Collecting all identifiers");
		Map<DataSet, Set<DataSetComponent<Identifier, ?, ?>>> ids = datasets.entrySet().stream()
			.filter(entryByKey(op -> op != refOperand))
			.map(Entry::getValue)
			.collect(toMap(identity(), ds -> ds.getMetadata().getIDs()));
		
		// TODO: Memory hungry!!! Find some way to stream instead of building this big
		// index collection
		LOGGER.trace("Indexing all datapoints");
		Map<JoinOperand, Map<Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint>> indexes = new HashMap<>();
		for (JoinOperand op: datasets.keySet())
			if (op != refOperand)
			{
				var index = tryWith(datasets.get(op)::stream, stream -> 
					// toMap instead of groupingBy because there's never more than one datapoint in each group
					(LOGGER.isTraceEnabled() ? stream.peek(dp -> LOGGER.trace("Indexing {}", dp)) : stream)
						.collect(toConcurrentMap(dp -> dp.getValues(Identifier.class), identity()))
				);
				indexes.put(op, index);
			}

		// Structure before applying any clause
		DataSetStructure structureBefore = datasets.values().stream().map(DataSet::getMetadata).flatMap(Set::stream).collect(toDataStructure());

		LOGGER.debug("Joining all datapoints");
		SerBinaryOperator<Lineage> enricher = lineage2Enricher(this);
		result = new FunctionDataSet<>(structureBefore, dataset -> dataset.stream().peek(refDP -> LOGGER.trace("Joining {}", refDP)).map(refDP -> {
			// Get all datapoints from other datasets (there is no more than 1 for each
			// dataset)
			List<DataPoint> otherDPs = datasets.entrySet().stream()
					.filter(entryByKey(op -> op != refOperand))
					.map(Entry::getKey)
					.map(op -> indexes.get(op).get(refDP.getValues(ids.get(datasets.get(op)), Identifier.class)))
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

	private DataSet joinCaseB1(MetadataRepository repo, Map<JoinOperand, DataSet> datasets, Set<DataSetComponent<?, ?, ?>> usingComponents, JoinOperand refOperand)
	{
		// Find out which component must be renamed
		datasets = renameBefore(repo, datasets);
		Map<JoinOperand, DataSetStructure> structures = datasets.entrySet().stream().map(keepingKey(DataSet::getMetadata)).collect(entriesToMap());

		DataSetStructure virtualStructure = virtualStructure(repo, structures, datasets.get(refOperand).getMetadata(), CASE_B1);
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
		// Determine homonymous components
		ConcurrentMap<DataSetComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
		Set<DataSetComponent<?, ?, ?>> allToBeRenamed = datasets.values().stream()
				.map(DataSet::getMetadata)
				.flatMap(d -> d.stream())
				.filter(c -> unique.putIfAbsent(c, TRUE) != null)
				.filter(c -> usingNames.isEmpty() ? c.is(NonIdentifier.class) : !usingNames.contains(c.getAlias()))
				.collect(toSet());

		datasets = new HashMap<>(datasets);
		if (!allToBeRenamed.isEmpty())
			for (JoinOperand op: datasets.keySet())
			{
				DataSet dataset = datasets.get(op);
				DataSetStructure oldStructure = dataset.getMetadata();
				Map<DataSetComponent<?, ?, ?>, DataSetComponent<?, ?, ?>> toBeRenamed = new HashMap<>();
				for (DataSetComponent<?, ?, ?> c: oldStructure)
					if (allToBeRenamed.contains(c))
						toBeRenamed.put(c, c.getRenamed(new MembershipAlias(op.getId(), c.getAlias())));
				
				if (!toBeRenamed.isEmpty())
				{
					// Determine new structure
					DataSetStructure newStructure = new DataSetStructureBuilder(oldStructure)
						.removeComponents(toBeRenamed.keySet())
						.addComponents(toBeRenamed.values())
						.build();
					LOGGER.trace("Dataset {} structure {} will be changed to {}", op.getId(), oldStructure, newStructure);

					// Rename components
					if (toBeRenamed.keySet().stream().allMatch(c -> c.is(NonIdentifier.class)))
						// (fast-track) Use mapKeepingKeys only if all renames are on non-identifiers 
						dataset = new NamedDataSet(op.getId(), dataset.mapKeepingKeys(newStructure, identity(), dp -> {
							Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(NonIdentifier.class));
							map.keySet().removeAll(toBeRenamed.keySet());
							for (DataSetComponent<?, ?, ?> c: toBeRenamed.keySet())
								map.put(toBeRenamed.get(c), dp.get(c));
							return map;
						}));
					else
						// (Slow-track) Use low-level stream.map() to rename datapoints when renames involve identifiers
						dataset = new NamedDataSet(op.getId(), new FunctionDataSet<>(newStructure, ds -> ds.stream().map(dp -> {
							DataPointBuilder builder = new DataPointBuilder(dp, DONT_SYNC).delete(toBeRenamed.keySet());
							for (DataSetComponent<?, ?, ?> c: toBeRenamed.keySet())
								builder.add(toBeRenamed.get(c), dp.get(c));
							return builder
								.addAll(dp.getValues(toBeRenamed.values()))
								.build(dp.getLineage(), newStructure);
						}), dataset));
					
					datasets.replace(op, dataset);
				}
				
			}
		
		return datasets;
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

	private Entry<JoinOperand, JoinCase> tryJoinCases(Map<JoinOperand, DataSetStructure> datasetsMeta)
	{
		// Determine the superset of all identifiers
		Set<DataSetComponent<Identifier, ?, ?>> allIDs = datasetsMeta.values().stream().flatMap(ds -> ds.getIDs().stream()).collect(toSet());

		// Get the common ids
		Set<DataSetComponent<?, ?, ?>> commonIDs = getCommonIDs(datasetsMeta.values());
		// The reference dataset, if it exists, is the one that contains the superset of all ids
		Optional<JoinOperand> refDataSet = datasetsMeta.entrySet().stream().filter(entryByValue(ds -> ds.containsAll(allIDs))).map(Entry::getKey).findFirst();

		if (usingNames.isEmpty())
		{
			// Case A
			
			if (operator == INNER_JOIN)
			{
				if (refDataSet.isEmpty())
					 throw new VTLException("inner_join case A requires one dataset to contain all the identifiers from all other datasets:"
						 	+ datasetsMeta.values().stream().map(DataSetStructure::getIDs).map(Objects::toString).collect(joining("\n    - ", "\n    - ", "\n")));
			}
			else if (operator == LEFT_JOIN || operator == FULL_JOIN)
			{
				// All datasets must have the same ids
				boolean sameIDs = true;
				DataSetStructure last = null;
				for (DataSetStructure ds: datasetsMeta.values())
				{
					if (last != null)
						sameIDs = sameIDs && ds.getIDs().equals(last.getIDs());
					last = ds;
				}
		
				if (!sameIDs)
					throw new VTLIncompatibleStructuresException(operator.toString().toLowerCase() + " case A requires all the input datasets to have the same identifiers",
							datasetsMeta.values().stream().map(DataSetStructure::getIDs).collect(toList()));
			}

			return new SimpleEntry<>(operator == INNER_JOIN ? refDataSet.get() : operands.get(0), CASE_A);
		}
		else
		{
			// using keys must appear as components in every dataset
			for (VTLAlias alias: usingNames)
				for (Entry<JoinOperand, DataSetStructure> e: datasetsMeta.entrySet())
				{
					Set<VTLAlias> aliases = e.getValue().stream().map(DataSetComponent::getAlias).collect(toSet());
					if (!aliases.contains(alias))
						throw new VTLException("Join operands must contain the using keys as components, but "
							+ alias + " was not found in " + e.getKey());
				}

			// case B1: The reference dataset exists and using keys are a subset of the common ids
			Set<DataSetComponent<Identifier, ?, ?>> usingIDs = allIDs.stream().filter(c -> usingNames.contains(c.getAlias())).collect(toSet());
			if (refDataSet.isPresent() && commonIDs.containsAll(usingIDs))
				// Case B1: case A with using keys as a subset of common IDs
				return new SimpleEntry<>(refDataSet.get(), CASE_B1);
			else if (operator == INNER_JOIN)
			{
				// Try case B2 for inner_join

				// all datasets must have the same identifiers except the reference one
				Map<DataSetComponent<Identifier, ?, ?>, Long> countIDs = null;
				int opsize = operands.size();
				refDataSet = Optional.empty();
				
				// try to find a suitable reference dataset
				for (int i = 0; i < opsize; i++)
				{
					int tryRef = i;
					countIDs = IntStream.range(0, opsize).filter(ci -> ci != tryRef)
						.mapToObj(operands::get)
						.map(datasetsMeta::get)
						.map(DataSetStructure::getIDs)
						.flatMap(Set::stream)
						.collect(groupingBy(identity(), counting()));
					if (countIDs.values().stream().mapToLong(Long::longValue).allMatch(l -> opsize - 1 == l))
					{
						refDataSet = Optional.of(operands.get(i));
						break;
					}
				}
				if (refDataSet.isEmpty())
					throw new VTLException("inner_join couldn't pick a reference dataset between:"
						+ datasetsMeta.values().stream().map(DataSetStructure::getIDs).map(Objects::toString).collect(joining("\n    - ", "\n    - ", "\n")));
				
				// The using clause specifies all and only the common ids of non-ref datasets
				if (countIDs == null || !usingIDs.equals(countIDs.keySet()))
					throw new VTLException("inner_join requires the using keys to match the common ids of non-reference datasets:"
						+ datasetsMeta.values().stream().map(DataSetStructure::getIDs).map(Objects::toString).collect(joining("\n    - ", "\n    - ", "\n")));

				return new SimpleEntry<>(refDataSet.get(), CASE_B2);
			}
			else if (operator == LEFT_JOIN)
			{
				// Try case B2 for left_join
				
				// refDataset is the leftmost one
				refDataSet = Optional.of(operands.get(0));
				
				// all datasets must have the same identifiers except the reference one
				int opsize = operands.size();
				Map<DataSetComponent<Identifier, ?, ?>, Long> countIDs = IntStream.range(1, opsize)
					.mapToObj(operands::get)
					.map(datasetsMeta::get)
					.map(DataSetStructure::getIDs)
					.flatMap(Set::stream)
					.collect(groupingBy(identity(), counting()));
				if (!countIDs.values().stream().mapToLong(Long::longValue).allMatch(l -> opsize - 1 == l))
					throw new VTLException("left_join requires all datasets except the leftmost one to have the same ids:"
						+ datasetsMeta.values().stream().map(DataSetStructure::getIDs).map(Objects::toString).collect(joining("\n    - ", "\n    - ", "\n")));

				// The using clause specifies all and only the common ids of non-ref datasets
				if (!usingIDs.equals(countIDs.keySet()))
					throw new VTLException("left_join requires the using keys to match the common ids of non-reference datasets:"
						+ datasetsMeta.values().stream().map(DataSetStructure::getIDs).map(Objects::toString).collect(joining("\n    - ", "\n    - ", "\n")));

				return new SimpleEntry<>(refDataSet.get(), CASE_B2);
			}
			else
				throw new VTLException(operator.toString().toLowerCase() + " cannot have a using clause.");
		}
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

	private DataSetStructure virtualStructure(MetadataRepository repo, Map<JoinOperand, DataSetStructure> datasetsMeta, DataSetStructure refDataSet, JoinCase joinCase)
	{
		Set<DataSetComponent<?, ?, ?>> usingComponents = getUsingComponents(refDataSet, datasetsMeta.values());

		// The result structure
		DataSetStructureBuilder builder = new DataSetStructureBuilder();

		// Determine homonymous components
		ConcurrentMap<DataSetComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
		Stream<DataSetComponent<?, ?, ?>> allComponents = datasetsMeta.values().stream()
			.flatMap(Collection::stream)
			.filter(c -> unique.putIfAbsent(c, TRUE) != null);

		if (operator == CROSS_JOIN)
			// rename all homonymous components
			;
		else if (operator == FULL_JOIN)
			// rename only homonymous non-identifier components
			allComponents = allComponents.filter(c -> c.is(NonIdentifier.class));
		else // left_join, inner_join
			// which component must be renamed depend on the join case
			switch (joinCase)
			{
				// Case A: rename only homonymous, non-identifier components
				case CASE_A: allComponents = allComponents.filter(c -> c.is(NonIdentifier.class));
					break;
				// Case B1: rename only homonymous components not mentioned as keys in using
				case CASE_B1: allComponents = allComponents.filter(c -> !usingComponents.contains(c));
					break;
				// Case B2: rename only homonymous non-identifier components not mentioned as keys in using
				case CASE_B2: allComponents = allComponents.filter(c -> !usingNames.contains(c.getAlias()) && !(c.is(Identifier.class) && refDataSet.contains(c)));
						// Also add all ids of the reference dataset
						builder.addComponents(refDataSet.getIDs());
				    break;
			}

		// Do the renaming
		Set<DataSetComponent<?, ?, ?>> toBeRenamed = allComponents.collect(toSet());
		LOGGER.debug(operator.toString().toLowerCase() + " renames: {}", toBeRenamed);

		for (Entry<JoinOperand, DataSetStructure> e : datasetsMeta.entrySet())
			for (DataSetComponent<?, ?, ?> c : e.getValue())
				if (toBeRenamed.contains(c))
					builder.addComponent(c.getRenamed(c.getAlias().in(e.getKey().getId())));
				else
					builder.addComponent(c);

		return builder.build();
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
	
			// determine the join case and the reference dataset
			DataSetStructure result;
			Entry<JoinOperand, JoinCase> caseParams = tryJoinCases(datasetsMeta);
			JoinOperand refOperand = caseParams.getKey();
			JoinCase joinCase = caseParams.getValue();
			result = virtualStructure(scheme.getRepository(), datasetsMeta, datasetsMeta.get(refOperand), joinCase);
			LOGGER.info("{}ing {}", operator, operands.stream().collect(toConcurrentMap(JoinOperand::getId, datasetsMeta::get)));
	
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
					.map(toEntryWithKey(c -> c.getAlias().split().getValue()))
					.collect(groupingByConcurrent(Entry::getKey, mapping(Entry::getValue, toSet())));
			
			ambiguousComps = new HashMap<>(ambiguousComps);
			ambiguousComps.values().removeIf(s -> s.size() < 2);
			Entry<VTLAlias, Set<DataSetComponent<?, ?, ?>>> ambiguousComp = ambiguousComps.isEmpty() ? null : ambiguousComps.entrySet().iterator().next();
	
			if (ambiguousComp != null)
				throw new VTLAmbiguousComponentException(ambiguousComp.getKey(), ambiguousComp.getValue());
			
			// de-alias components for the final structure
			DataSetStructureBuilder builder = new DataSetStructureBuilder();
			for (DataSetComponent<?, ?, ?> comp: result)
				if (comp.getAlias().isComposed())
					builder = builder.addComponent(comp.getRenamed(comp.getAlias().getMemberAlias()));
				else
					builder = builder.addComponent(comp);
			
			DataSetStructure structure = builder.build();
			
			for (DataSetComponent<?, ?, ?> comp: structure)
				if (comp.getAlias().isComposed())
					throw new IllegalStateException();
			
			return structure;
		}
		catch (RuntimeException e)
		{
			throw new VTLNestedException("In expression " + this, e);
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