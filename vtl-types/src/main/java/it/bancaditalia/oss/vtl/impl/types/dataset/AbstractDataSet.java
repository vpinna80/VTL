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
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerFunction.identity;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.groupingByConcurrent;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collector;
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
		return dataStructure instanceof DataSetMetadata ? (DataSetMetadata) dataStructure : null;
	}

	@Override
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, SerBiPredicate<DataPoint, DataPoint> predicate, SerBinaryOperator<DataPoint> mergeOp)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> commonIds = getMetadata().getComponents(Identifier.class);
		commonIds.retainAll(other.getComponents(Identifier.class));
		
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, List<DataPoint>> index;
		try (Stream<DataPoint> stream = other.stream())
		{
			// performance if
			if (commonIds.equals(other.getComponents(Identifier.class)))
				index = stream.collect(toConcurrentMap(dp -> dp.getValues(commonIds, Identifier.class), Collections::singletonList));
			else
				index = stream.collect(groupingByConcurrent(dp -> dp.getValues(commonIds, Identifier.class)));
		}
		
		return filteredMappedJoinWithIndex(this, metadata, predicate, mergeOp, commonIds, index);
	}

	protected static DataSet filteredMappedJoinWithIndex(DataSet streamed, DataSetMetadata metadata, BiPredicate<DataPoint, DataPoint> predicate, BinaryOperator<DataPoint> mergeOp,
			Set<DataStructureComponent<Identifier, ?, ?>> commonIds,
			Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, ? extends Collection<DataPoint>> index)
	{
		return new LightFDataSet<>(metadata, d -> {
				final Stream<DataPoint> stream = d.stream();
				return stream.map(dpThis -> {
						Collection<DataPoint> otherSubGroup = index.get(dpThis.getValues(commonIds, Identifier.class));
						if (otherSubGroup == null)
							return Stream.<DataPoint>empty();
						else
							return otherSubGroup.stream()
								.filter(dpOther -> predicate.test(dpThis, dpOther))
								.map(dpOther -> mergeOp.apply(dpThis, dpOther)); 
					}).collect(concatenating(ORDERED))
					.onClose(stream::close);
			}, streamed);
	}

	@Override
	public DataSet mapKeepingKeys(DataSetMetadata metadata,
			SerFunction<? super DataPoint, ? extends Lineage> lineageOperator, SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		final Set<DataStructureComponent<Identifier, ?, ?>> identifiers = dataStructure.getComponents(Identifier.class);
		if (!metadata.getComponents(Identifier.class).equals(identifiers))
			throw new VTLInvariantIdentifiersException("map", identifiers, metadata.getComponents(Identifier.class));
		
		LOGGER.trace("Creating dataset by mapping from {} to {}", dataStructure, metadata);
		
		SerUnaryOperator<DataPoint> extendingOperator = dp -> new DataPointBuilder(dp.getValues(Identifier.class))
				.addAll(operator.apply(dp))
				.build(lineageOperator.apply(dp), metadata);
		
		return new AbstractDataSet(metadata)
		{
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.stream().map(extendingOperator);
			}
			
			@Override
			public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter,
					SerCollector<DataPoint, A, TT> groupCollector,
					SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
			{
				return AbstractDataSet.this.streamByKeys(keys, filter, mapping(extendingOperator, groupCollector), finisher);
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
		final Set<Characteristics> characteristics = new HashSet<>(groupCollector.characteristics());
		characteristics.remove(IDENTITY_FINISH);
		Collector<Entry<DataPoint, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>>, A, T> decoratedCollector = Collector.of(
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
				},
				// finisher
				acc -> groupCollector.finisher().andThen(tt -> finisher.apply(tt, keyValues.get(acc))).apply(acc),
				// characteristics
				characteristics.toArray(new Characteristics[0]));
		
		try (Stream<DataPoint> stream = stream())
		{
			ConcurrentMap<?, T> result = stream
					.filter(dp -> dp.matches(filter))
					.map(toEntryWithValue(dp -> dp.getValues(keys, Identifier.class)))
					.collect(groupingByConcurrent(e -> e.getValue(), decoratedCollector))
					;
			
			return Utils.getStream(result.values());
		}
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
				try (Stream<DataPoint> stream = AbstractDataSet.this.stream())
				{
					if (cache == null)
						cache = stream
								.collect(groupingByConcurrent(dp -> dp.getValues(keys, Identifier.class), groupCollector))
								.entrySet();

					return Utils.getStream(cache)
							.map(splitting((k, v) -> finisher.apply(v, k)));
				}
			}
		};
	}

	@Override
	public <TT> DataSet analytic(
			Map<DataStructureComponent<Measure, ?, ?>, DataStructureComponent<Measure, ?, ?>> components,
			WindowClause clause,
			Map<DataStructureComponent<Measure, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors,
			Map<DataStructureComponent<Measure, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>>> finishers)
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
				(a, b) -> { a.addAll(b); return a; }, identity(), EnumSet.of(CONCURRENT, IDENTITY_FINISH, UNORDERED));
		
		DataSetMetadata newStructure = new DataStructureBuilder(getMetadata())
				.removeComponents(components.keySet())
				.addComponents(components.values())
				.build();
		
		return new AbstractDataSet(newStructure) {
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.streamByKeys(ids, toSortedSet, (group, keyValues) -> {
						DataPoint[] sorted = group.toArray(new DataPoint[group.size()]);
						
						IntStream indexes = IntStream.range(0, sorted.length);
						if (!Utils.SEQUENTIAL)
							indexes = indexes.parallel();
						
						Stream<DataPoint> result = indexes.mapToObj(index ->
								Utils.getStream(components)
									.map(splitting((oldC, newC) -> {  
										try {
											Stream<DataPoint> window;
											if (index + inf >= sorted.length || index + sup < 0)
												window = Stream.empty();
											else
												window = Arrays.stream(sorted, max(0, safeSum(index, inf)), min(safeInc(safeSum(index, sup)), sorted.length));
											
											return window.collect(collectingAndThen(mapping(dp -> dp.get(oldC), collectors.get(oldC)), v -> new SimpleEntry<>(newC, finishers.get(oldC).apply(v, sorted[index].get(oldC)))));
										}
										catch (RuntimeException e)
										{
											throw e;
										}
									})).collect(collectingAndThen(entriesToMap(), map -> new SimpleEntry<>(map, sorted[index]))))
								.map(splitting((values, dp) -> new DataPointBuilder(dp)
									.delete(components.keySet())
									.addAll(values)
									.build(dp.getLineage(), newStructure)));
						
						return result;
					}).flatMap(identity());
			}
		};
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
		return new LightDataSet(dataStructure, () -> stream().filter(predicate));
	}

	@Override
	public String toString()
	{
		return "#UNNAMED#" + getMetadata();
	}
	
	@Override
	public final Stream<DataPoint> stream()
	{
		LOGGER.trace("Streaming dataset of {}", dataStructure);

		return streamDataPoints();
	}

	protected abstract Stream<DataPoint> streamDataPoints();
}
