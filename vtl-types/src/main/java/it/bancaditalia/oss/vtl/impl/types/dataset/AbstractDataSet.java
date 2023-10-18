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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.RANGE;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentSet;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collector.Characteristics;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerConsumer;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public abstract class AbstractDataSet implements DataSet
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataSet.class);

	private final DataSetMetadata dataStructure;

	protected AbstractDataSet(DataSetMetadata dataStructure)
	{
		this.dataStructure = dataStructure;
	}
	
	@Override
	public DataSet membership(String alias)
	{
		final DataSetMetadata membershipStructure = dataStructure.membership(alias);
		LOGGER.trace("Creating dataset by membership on {} from {} to {}", alias, dataStructure, membershipStructure);
		
		DataStructureComponent<?, ?, ?> sourceComponent = dataStructure.getComponent(alias)
				.orElseThrow((Supplier<? extends RuntimeException> & Serializable) () -> new VTLMissingComponentsException(alias, dataStructure));
		DataStructureComponent<? extends NonIdentifier, ?, ?> membershipMeasure = membershipStructure.getComponents(Measure.class).iterator().next();

		SerFunction<DataPoint, Map<DataStructureComponent<? extends NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> operator = 
				dp -> singletonMap(membershipMeasure, dp.get(sourceComponent));
		
		return mapKeepingKeys(membershipStructure, dp -> LineageNode.of("#" + alias, dp.getLineage()), operator);
	}
	
	@Override
	public DataSet subspace(Map<? extends DataStructureComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keyValues, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator)
	{
		DataSetMetadata newMetadata = new DataStructureBuilder(dataStructure).removeComponents(keyValues.keySet()).build();
		
		return new AbstractDataSet(newMetadata)
		{
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.stream()
						.filter(dp -> dp.matches(keyValues))
						.map(dp -> new DataPointBuilder(dp)
								.delete(keyValues.keySet())
								.build(lineageOperator.apply(dp), newMetadata));
			}
		};
	}

	@Override
	public Optional<DataStructureComponent<?, ?, ?>> getComponent(String name)
	{
		return dataStructure.getComponent(name);
	}

	@Override
	public DataSetMetadata getMetadata()
	{
		return dataStructure;
	}

	@Override
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, SerBiPredicate<DataPoint, DataPoint> predicate, SerBinaryOperator<DataPoint> mergeOp, boolean leftJoin)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> ids = getComponents(Identifier.class);
		Set<DataStructureComponent<Identifier, ?, ?>> otherIds = other.getComponents(Identifier.class);
		Set<DataStructureComponent<Identifier, ?, ?>> commonIds = new HashSet<>(ids);
		commonIds.retainAll(otherIds);
		
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> index;
		try (Stream<DataPoint> stream = other.stream())
		{
			if (otherIds.equals(ids))
				// more performance if joining over all keys
				index = stream.collect(toConcurrentMap(dp -> dp.getValues(commonIds, Identifier.class), Collections::singleton));
			else
				index = stream.collect(groupingByConcurrent(dp -> dp.getValues(commonIds, Identifier.class), toConcurrentSet()));
		}
		
		return new AbstractDataSet(metadata)
		{
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.stream()
						.map(dpThis -> flatMapDataPoint(predicate, mergeOp, commonIds, index, leftJoin, dpThis))
						.collect(concatenating(ORDERED));
			}
		};
	}

	protected static Stream<DataPoint> flatMapDataPoint(BiPredicate<DataPoint, DataPoint> predicate,
			BinaryOperator<DataPoint> mergeOp, Set<DataStructureComponent<Identifier, ?, ?>> commonIds,
			Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, ? extends Collection<DataPoint>> indexed,
			boolean leftJoin, DataPoint dpThis)
	{
		Collection<DataPoint> otherSubGroup = indexed.get(dpThis.getValues(commonIds, Identifier.class));
		if (otherSubGroup == null)
			if (leftJoin)
				return Stream.of(mergeOp.apply(dpThis, null));
			else
				return Stream.<DataPoint>empty();
		else
			return otherSubGroup.stream()
				.filter(dpOther -> predicate.test(dpThis, dpOther))
				.map(dpOther -> mergeOp.apply(dpThis, dpOther));
	}

	@Override
	public DataSet mapKeepingKeys(DataSetMetadata metadata, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator, 
			SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		final Set<DataStructureComponent<Identifier, ?, ?>> identifiers = dataStructure.getComponents(Identifier.class);
		if (!metadata.getComponents(Identifier.class).containsAll(identifiers))
			throw new VTLInvariantIdentifiersException("map", identifiers, metadata.getComponents(Identifier.class));
		
		LOGGER.trace("Creating dataset by mapping from {} to {}", dataStructure, metadata);
		
		SerUnaryOperator<DataPoint> extendingOperator = dp -> new DataPointBuilder(dp.getValues(Identifier.class))
				.addAll(operator.apply(dp))
				.build(lineageOperator.apply(dp), metadata);
		
		return new MappedDataSet(metadata, this, extendingOperator);
	}

	@Override
	public DataSet flatmapKeepingKeys(DataSetMetadata metadata, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Stream<? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>>> operator)
	{
		final Set<DataStructureComponent<Identifier, ?, ?>> identifiers = dataStructure.getComponents(Identifier.class);
		if (!metadata.getComponents(Identifier.class).containsAll(identifiers))
			throw new VTLInvariantIdentifiersException("map", identifiers, metadata.getComponents(Identifier.class));
		
		LOGGER.trace("Creating dataset by mapping from {} to {}", dataStructure, metadata);
		
		return new AbstractDataSet(metadata) {
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.stream()
						.map(dp -> {
							Lineage lineage = lineageOperator.apply(dp);
							return operator.apply(dp)
								.map(map -> new DataPointBuilder(dp.getValues(Identifier.class))
										.addAll(map)
										.build(lineage, metadata));
						}).collect(concatenating(ORDERED));
			}
		};
	}
	
	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter,
			SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		// key group holder
		final Map<A, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> keyValues = new ConcurrentHashMap<>();
		
		// Decorated collector that keeps track of grouping key values for the finisher
		Set<Characteristics> characteristics = EnumSet.copyOf(groupCollector.characteristics());
		characteristics.remove(IDENTITY_FINISH);
		SerCollector<Entry<DataPoint, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>>, A, T> decoratedCollector = SerCollector.of(
				// supplier
				groupCollector.supplier(),
				// accumulator
				(acc, entry) -> {
					groupCollector.accumulator().accept(acc, entry.getKey());
					keyValues.putIfAbsent(acc, entry.getValue());
				}, // combiner
				(accLeft, accRight) -> {
					A combinedAcc = groupCollector.combiner().apply(accLeft, accRight);
					keyValues.putIfAbsent(combinedAcc, keyValues.get(accLeft));
					keyValues.putIfAbsent(combinedAcc, keyValues.get(accRight));
					return combinedAcc;
				}, // finisher
				acc -> groupCollector.finisher().andThen(tt -> finisher.apply(tt, keyValues.get(acc))).apply(acc),
				// characteristics
				characteristics);
		
		Collection<T> result;
		try (Stream<DataPoint> stream = stream())
		{
			result = stream
					.filter(dp -> dp.matches(filter))
					.map(toEntryWithValue(dp -> dp.getValues(keys, Identifier.class)))
					.collect(groupingByConcurrent(Entry::getValue, decoratedCollector))
					.values();
		}
			
		return Utils.getStream(result);
	}

	@Override
	public DataSet aggr(DataSetMetadata structure, Set<DataStructureComponent<Identifier, ?, ?>> keys,
			SerCollector<DataPoint, ?, DataPoint> groupCollector,
			SerBiFunction<DataPoint, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> finisher)
	{
		return new AbstractDataSet(structure) {
			private static final long serialVersionUID = 1L;
			private transient Set<Entry<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint>> cache = null;
			
			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				createCache(keys, groupCollector);

				return Utils.getStream(cache)
						.map(splitting((k, v) -> finisher.apply(v, k)));
			}

			private synchronized void createCache(Set<DataStructureComponent<Identifier, ?, ?>> keys,
					SerCollector<DataPoint, ?, DataPoint> groupCollector)
			{
				if (cache == null)
					try (Stream<DataPoint> stream = AbstractDataSet.this.stream())
					{
						cache = stream
								.collect(groupingByConcurrent(dp -> dp.getValues(keys, Identifier.class), groupCollector))
								.entrySet();
					}
			}
		};
	}

	@Override
	public <TT> DataSet analytic(SerFunction<DataPoint, Lineage> lineageOp, 
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components,
			WindowClause clause,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors,
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers)
	{
		if (clause.getWindowCriterion() != null && clause.getWindowCriterion().getType() == RANGE)
			throw new UnsupportedOperationException("Range windows are not implemented in analytic invocation");

		DataSetMetadata newStructure = new DataStructureBuilder(getMetadata())
				.removeComponents(components.keySet())
				.addComponents(components.values())
				.build();
		
		return new AnalyticDataSet<>(this, newStructure, lineageOp, clause, collectors, finishers, components);
	}
	
	@Override
	public DataSet union(SerFunction<DataPoint, Lineage> lineageOp, List<DataSet> others, boolean unionAll)
	{
		// Fast track when the functional aspect is preserved
		if (unionAll)
			return new AbstractDataSet(dataStructure) {
				private static final long serialVersionUID = 1L;

				@Override
				protected Stream<DataPoint> streamDataPoints()
				{
					return Stream.concat(Stream.of(AbstractDataSet.this), others.stream()).map(DataSet::stream).collect(concatenating(ORDERED));
				}
			};
		
		Set<DataStructureComponent<Identifier, ?, ?>> ids = dataStructure.getComponents(Identifier.class);
		for (DataSet other: others)
			if (!dataStructure.equals(other.getMetadata()))
				throw new InvalidParameterException("Union between two datasets with different structures: " + dataStructure + " and " + other.getMetadata()); 

		List<Set<DataPoint>> results = new ArrayList<>();
		Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> seen = newKeySet();
		LOGGER.info("Evaluating first union operand");
		try (Stream<DataPoint> stream = stream())
		{
			results.add(stream
					.peek(dp -> seen.add(dp.getValues(ids)))
					.map(dp -> dp.enrichLineage(l -> lineageOp.apply(dp)))
					.collect(toConcurrentSet())
			);
		}
		
		// eagerly compute the differences (one set at a time to avoid OutOfMemory and preserve "leftmost rule")
		int nOperand = 2;
		for (DataSet other: others)
			try (Stream<DataPoint> stream = other.stream())
			{
				LOGGER.info("Evaluating union operand {}", nOperand++);
				Stream<DataPoint> stream2 = stream;
				if (LOGGER.isTraceEnabled())
					stream2 = stream2.peek(dp -> {
						if (seen.contains(dp.getValues(ids)))
							LOGGER.trace("Union: Found a duplicated datapoint in {}: {}", other, dp);
					});

				results.add(stream2
						.filter(dp -> seen.add(dp.getValues(ids)))
						.map(dp -> dp.enrichLineage(l -> lineageOp.apply(dp)))
						.collect(toConcurrentSet()));
			}
		
		// concat all datapoints from all sets
		return new FunctionDataSet<>(dataStructure, list -> Utils.getStream(results)
				.map(Utils::getStream)
				.collect(concatenating(ORDERED)), results);
	}

	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate, SerUnaryOperator<Lineage> lineageOperator)
	{
		return new StreamWrapperDataSet(dataStructure, () -> 
			stream()
			.filter(predicate)
			.map(dp -> new DataPointBuilder(dp)
					.build(lineageOperator.apply(dp.getLineage()), dataStructure)));
	}
	
	@Override
	public String toString()
	{
		return "#UNNAMED#" + getMetadata();
	}
	
	@Override
	public final Stream<DataPoint> stream()
	{
		LOGGER.debug("Requested streaming of {}", this);
		
		Stream<DataPoint> stream = streamDataPoints();
		if (LOGGER.isTraceEnabled() && (!(this instanceof NamedDataSet) || !((NamedDataSet) this).getAlias().startsWith("csv:")))
		{
			AtomicBoolean dontpeek = new AtomicBoolean(false);
			Map<Map<?, ?>, Integer> seen = new HashMap<>();
			stream = stream.peek(dp -> {
					if (!dontpeek.get() && !dp.getLineage().toString().startsWith("csv:"))
					{
						if (this instanceof NamedDataSet)
							LOGGER.trace("Dataset {} output datapoint {}", ((NamedDataSet) this).getAlias(),  dp);
						else
							LOGGER.trace("Dataset {} output datapoint {}", dp.getLineage(), dp);
						Map<?, ?> keyVals = dp.getValues(Identifier.class);
						int times = seen.merge(keyVals, 1, Integer::sum);
						if (times > 1)
							throw new IllegalStateException("Keys " + keyVals + " appear " + times + " times.");
					}
					else
						dontpeek.set(true);
				});
		}
		
		return stream.onClose(() -> LOGGER.trace("Closing stream for {}", this));
	}

	protected abstract Stream<DataPoint> streamDataPoints();

	@Override
	public DataSet setDiff(DataSet right)
	{
		return new FunctionDataSet<>(getMetadata(), r -> {
			Set<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> index;
			try (Stream<DataPoint> stream = r.stream())
			{
				index = stream.map(dp -> dp.getValues(Identifier.class))
						.collect(toConcurrentSet());
			}
			
			return peekIfTrace(dp -> {
					final Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keys = dp.getValues(Identifier.class);
					LOGGER.trace("Setdiff: {} datapoint with keys {}.", index.contains(keys) ? "removed" : "passed", keys);
				}).filter(dp -> !index.contains(dp.getValues(Identifier.class)), SerUnaryOperator.identity()).stream();
		}, right);
	}
	
	private AbstractDataSet peekIfTrace(SerConsumer<DataPoint> consumer)
	{
		if (!LOGGER.isTraceEnabled())
			return this;
		
		return new AbstractDataSet(dataStructure) 
		{
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.stream().sequential().peek(consumer);
			}
		};
	}
}
