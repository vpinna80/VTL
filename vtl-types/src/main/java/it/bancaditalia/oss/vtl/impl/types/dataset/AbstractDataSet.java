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

import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.RANGE;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentSet;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithKey;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collector.Characteristics;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
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
	public DataSet membership(String alias, Lineage lineage)
	{
		final DataSetMetadata membershipStructure = dataStructure.membership(alias);
		LOGGER.trace("Creating dataset by membership on {} from {} to {}", alias, dataStructure, membershipStructure);
		
		DataStructureComponent<?, ?, ?> sourceComponent = dataStructure.getComponent(alias)
				.orElseThrow((Supplier<? extends RuntimeException> & Serializable) () -> new VTLMissingComponentsException(alias, dataStructure));
		DataStructureComponent<? extends NonIdentifier, ?, ?> membershipMeasure = membershipStructure.getComponents(Measure.class).iterator().next();

		SerFunction<DataPoint, Map<DataStructureComponent<? extends NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> operator = 
				dp -> singletonMap(membershipMeasure, dp.get(sourceComponent));
		
		return mapKeepingKeys(membershipStructure, dp -> lineage, operator);
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
				try
				{
					index = stream.collect(toConcurrentMap(dp -> dp.getValues(commonIds, Identifier.class), Collections::singleton));
				}
				catch (RuntimeException e)
				{
					throw e;
				}
			else
				index = stream.collect(groupingByConcurrent(dp -> dp.getValues(commonIds, Identifier.class), toConcurrentSet()));
		}
		
		return filteredMappedJoinWithIndex(this, metadata, predicate, mergeOp, commonIds, index, leftJoin);
	}

	protected static DataSet filteredMappedJoinWithIndex(DataSet streamed, DataSetMetadata metadata, BiPredicate<DataPoint, DataPoint> predicate, 
			BinaryOperator<DataPoint> mergeOp, Set<DataStructureComponent<Identifier, ?, ?>> commonIds, 
			Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, ? extends Collection<DataPoint>> indexed, boolean leftJoin)
	{
		return flatMapInternal(streamed, metadata, dpThis -> {
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
				});
	}

	@Override
	public DataSet mapKeepingKeys(DataSetMetadata metadata, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator, 
			SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		final Set<DataStructureComponent<Identifier, ?, ?>> identifiers = dataStructure.getComponents(Identifier.class);
		if (!metadata.getComponents(Identifier.class).equals(identifiers))
			throw new VTLInvariantIdentifiersException("map", identifiers, metadata.getComponents(Identifier.class));
		
		LOGGER.trace("Creating dataset by mapping from {} to {}", dataStructure, metadata);
		
		SerUnaryOperator<DataPoint> extendingOperator = dp -> new DataPointBuilder(dp.getValues(Identifier.class))
				.addAll(operator.apply(dp))
				.build(lineageOperator.apply(dp), metadata);
		
		return mapInternal(this, metadata, extendingOperator);
	}

	private static AbstractDataSet mapInternal(DataSet source, DataSetMetadata metadata, SerUnaryOperator<DataPoint> operator)
	{
		return new AbstractDataSet(metadata)
		{
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return source.stream().map(operator);
			}
			
			@Override
			public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter,
					SerCollector<DataPoint, A, TT> groupCollector,
					SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
			{
				return source.streamByKeys(keys, filter, mapping(operator, groupCollector), finisher);
			}
		};
	}

	private static AbstractDataSet flatMapInternal(DataSet source, DataSetMetadata metadata, SerFunction<DataPoint, Stream<DataPoint>> operator)
	{
		return new AbstractDataSet(metadata)
		{
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return source.stream().map(operator).collect(concatenating(ORDERED));
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
	public <TT> DataSet aggr(DataSetMetadata structure, Set<DataStructureComponent<Identifier, ?, ?>> keys,
			SerCollector<DataPoint, ?, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> finisher)
	{
		return new AbstractDataSet(structure) {
			private static final long serialVersionUID = 1L;
			private transient Set<Entry<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT>> cache = null;
			
			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				createCache(keys, groupCollector);

				return Utils.getStream(cache)
						.map(splitting((k, v) -> finisher.apply(v, k)));
			}

			private synchronized void createCache(Set<DataStructureComponent<Identifier, ?, ?>> keys,
					SerCollector<DataPoint, ?, TT> groupCollector)
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
	public <TT> DataSet analytic(
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components,
			WindowClause clause,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors,
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers)
	{
		if (clause.getWindowCriterion() != null && clause.getWindowCriterion().getType() == RANGE)
			throw new UnsupportedOperationException("Range windows are not implemented in analytic invocation");

		Set<DataStructureComponent<Identifier, ?, ?>> ids = clause.getPartitioningIds();
		
		Comparator<DataPoint> comparator = (dp1, dp2) -> {
				for (SortCriterion criterion: clause.getSortCriteria())
				{
					int res = dp1.get(criterion.getComponent()).compareTo(dp2.get(criterion.getComponent()));
					if (res != 0)
						return criterion.getMethod() == ASC ? res : -res;
				}

				return 0;
			};

		int inf, sup;
		if (clause.getWindowCriterion() != null)
		{
			LimitCriterion infBound = clause.getWindowCriterion().getInfBound();
			LimitCriterion supBound = clause.getWindowCriterion().getSupBound();
			inf = (infBound.getDirection() == PRECEDING ? -1 : 1) * (int) infBound.getCount(); 
			sup = (supBound.getDirection() == PRECEDING ? -1 : 1) * (int) supBound.getCount();
		}
		else
		{
			inf = Integer.MIN_VALUE;
			sup = Integer.MAX_VALUE;
		}
		
		SerCollector<DataPoint, ConcurrentSkipListSet<DataPoint>, ConcurrentSkipListSet<DataPoint>> toSortedSet = SerCollector.of(
				() -> new ConcurrentSkipListSet<>(comparator), ConcurrentSkipListSet::add, 
				(a, b) -> { a.addAll(b); return a; }, EnumSet.of(CONCURRENT, IDENTITY_FINISH, UNORDERED));
		
		DataSetMetadata newStructure = new DataStructureBuilder(getMetadata())
				.removeComponents(components.keySet())
				.addComponents(components.values())
				.build();
		
		return new AbstractDataSet(newStructure) {
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.streamByKeys(ids, toSortedSet, (part, keyValues) -> {
						DataPoint[] partArray = part.toArray(new DataPoint[part.size()]);
						
						IntStream indexes = IntStream.range(0, partArray.length);
						if (!Utils.SEQUENTIAL)
							indexes = indexes.parallel();
						
						return indexes.mapToObj(index -> {
							final int safeInf = max(0, safeSum(index, inf));
							final int safeSup = min(safeInc(safeSum(index, sup)), partArray.length);

							return Utils.getStream(components)
								.map(splitting((oldC, newC) -> {  
									// stream the array slice containing all the datapoints in current window
									Stream<DataPoint> window = Stream.empty();
									if (index + inf < partArray.length && index + sup >= 0)
									{
										window = Arrays.stream(partArray, safeInf, safeSup);
										if (!Utils.SEQUENTIAL)
											window = window.parallel();
									}
									else
										window = Stream.empty();
									
									// Compute the analytic invocation over current window and measure and pair the result with the old value
									return window.collect(collectingAndThen(mapping(dp -> dp.get(oldC), collectors.get(oldC)), 
										v -> new SimpleEntry<>(newC, finishers.get(oldC).apply(v, partArray[index].get(oldC)))));
								})).collect(collectingAndThen(entriesToMap(), toEntryWithValue(key -> partArray[index])));
							}).map(splitting((colls, dp) -> Utils.getStream(colls)
								.map(coll -> Utils.getStream(coll.getValue()).map(toEntryWithKey(v -> coll.getKey())).collect(toList()))
								// Generate all possible combinations of measure results of the window function
								.collect(SerCollector.of(
										// supplier: long line because of eclipse bad type inference...
										() -> (Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>>) ConcurrentHashMap.<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>>newKeySet(), 
										// Accumulator: generate a map for each measure value
										(a, l) -> l.stream().forEach(e -> {
												Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> measure = new HashMap<>();
												measure.put(e.getKey(), e.getValue());
												a.add(measure);
											}), 
										// Combiner: generate a new map combining all measure values from the first map to all measure values of the second
										(a1, a2) -> a1.stream()
											.map(m1v -> a2.stream()
												.map(m2v -> {
													Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> a = new HashMap<>(m1v);
													a.putAll(m2v);
													return a;
												}))
											.collect(concatenating(ORDERED))
										.collect(toConcurrentSet()), 
										EnumSet.of(CONCURRENT, UNORDERED))
								).stream()
								.map(toEntryWithValue(map -> dp))
							)).collect(concatenating(ORDERED))
							.map(splitting((values, dp) -> new DataPointBuilder(dp)
								.delete(components.keySet())
								.addAll(values)
								.build(dp.getLineage(), newStructure)));
					}).collect(concatenating(ORDERED));
			}
		};
	}
	
	@Override
	public DataSet union(DataSet... others)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> ids = dataStructure.getComponents(Identifier.class);
		for (DataSet other: others)
			if (!dataStructure.equals(other.getMetadata()))
				throw new InvalidParameterException("Union between two datasets with different structures: " + dataStructure + " and " + other.getMetadata()); 

		List<Set<DataPoint>> results = new ArrayList<>();
		Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> seen = newKeySet();
		try (Stream<DataPoint> stream = stream())
		{
			results.add(stream.peek(dp -> seen.add(dp.getValues(ids))).collect(toConcurrentSet()));
		}
		
		// eagerly compute the differences (one set at a time to avoid OutOfMemory)
		for (DataSet other: others)
			try (Stream<DataPoint> stream = other.stream())
			{
				results.add(stream.filter(dp -> seen.add(dp.getValues(ids))).collect(toConcurrentSet()));
			}
		
		// concat all datapoints from all sets
		return new FunctionDataSet<>(dataStructure, list -> Utils.getStream(results)
				.map(Utils::getStream)
				.collect(concatenating(ORDERED)), results);
	}
	
	/*
	 * Detects overflows in sum and caps it to Integer.MAX_VALUE 
	 */
	private static int safeSum(int x, int y)
	{
		int r = x + y;
		return ((x ^ r) & (y ^ r)) < 0 ? Integer.MAX_VALUE : r;
	}
	
	private static int safeInc(int a)
	{
		return safeSum(a, 1);
	}
	
	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate)
	{
		return new StreamWrapperDataSet(dataStructure, () -> stream().filter(predicate));
	}

	@Override
	public String toString()
	{
		return "#UNNAMED#" + getMetadata();
	}
	
	@Override
	public final Stream<DataPoint> stream()
	{
		LOGGER.debug("Starting stream for {}", this);
		return streamDataPoints().onClose(() -> LOGGER.trace("Closing stream for {}", this));
	}

	protected abstract Stream<DataPoint> streamDataPoints();

	@Override
	public DataSet setDiff(DataSet right)
	{
		return new BiFunctionDataSet<>(getMetadata(), (l, r) -> {
			Set<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> index;
			try (Stream<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> stream = r.stream().map(dp -> dp.getValues(Identifier.class)))
			{
				index = stream.collect(toConcurrentSet());
			}
			
			return filter(dp -> !index.contains(dp.getValues(Identifier.class))).stream();
		}, this, right);
	}
}
