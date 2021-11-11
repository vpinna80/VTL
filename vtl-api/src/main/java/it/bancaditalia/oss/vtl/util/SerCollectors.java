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
package it.bancaditalia.oss.vtl.util;

import static it.bancaditalia.oss.vtl.util.SerFunction.identity;
import static java.util.Collections.emptySet;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collector.Characteristics;

public class SerCollectors
{
    public static <T, K, U> SerCollector<T, ConcurrentMap<K, U>, ConcurrentMap<K, U>> toConcurrentMap(SerFunction<? super T, ? extends K> keyMapper,
                                                        SerFunction<? super T, ? extends U> valueMapper)
    {
        return toConcurrentMap(keyMapper, valueMapper, throwingMerger(), ConcurrentHashMap::new);
    }

    public static <T, K, U> SerCollector<T, ConcurrentMap<K, U>, ConcurrentMap<K, U>> toConcurrentMap(SerFunction<? super T, ? extends K> keyMapper,
                    SerFunction<? super T, ? extends U> valueMapper, SerBinaryOperator<U> mergeFunction)
    {
        return toConcurrentMap(keyMapper, valueMapper, mergeFunction, ConcurrentHashMap::new);
    }

    public static <T, K, U, M extends ConcurrentMap<K, U>> SerCollector<T, M, M> toConcurrentMap(SerFunction<? super T, ? extends K> kMapper,
    		SerFunction<? super T, ? extends U> vMapper, SerBinaryOperator<U> mergeFun, SerSupplier<M> mapSupplier)
    {
        SerBiConsumer<M, T> acc = (map, e) -> map.merge(kMapper.apply(e), vMapper.apply(e), mergeFun);
                                              
        return SerCollector.of(mapSupplier, acc, mapMerger(mergeFun), identity(), EnumSet.of(CONCURRENT, UNORDERED, IDENTITY_FINISH));
    }

    public static <T, A, R, RR> SerCollector<T, A, RR> collectingAndThen(SerCollector<T, A, ? extends R> downstream, SerFunction<? super R, RR> finisher)
	{
		Set<Characteristics> characteristics = downstream.characteristics();
		if (characteristics.contains(IDENTITY_FINISH))
		{
			if (characteristics.size() == 1)
				characteristics = Collections.emptySet();
			else
			{
				characteristics = EnumSet.copyOf(characteristics);
				characteristics.remove(IDENTITY_FINISH);
			}
		}
		return new SerCollector<>(downstream.supplier(), downstream.accumulator(), downstream.combiner(),
				downstream.finisher().andThen(finisher), characteristics);
	}
	
    public static <T> SerCollector<T, Set<T>, Set<T>> toSet()
    {
        return new SerCollector<>(HashSet::new, Set::add, (left, right) -> { left.addAll(right); return left; }, 
        		identity(), EnumSet.of(UNORDERED, IDENTITY_FINISH));
    }

    public static <T, C extends Collection<T>> SerCollector<T, C, C> toCollection(SerSupplier<C> collectionFactory)
    {
        return SerCollector.of(collectionFactory, Collection::add, (r1, r2) -> { r1.addAll(r2); return r1; }, identity(), EnumSet.of(IDENTITY_FINISH));
    }

    public static <T> SerCollector<T, Long[], Long> counting()
    {
        return reducing(0L, e -> 1L, Long::sum);
    }

    public static <T, U> SerCollector<T, U[], U> reducing(U identity, SerFunction<? super T, ? extends U> mapper, SerBinaryOperator<U> op)
    {
        return new SerCollector<>(boxSupplier(identity),
                (a, t) -> { a[0] = op.apply(a[0], mapper.apply(t)); },
                (a, b) -> { a[0] = op.apply(a[0], b[0]); return a; },
                a -> a[0], emptySet());
    }

    public static <T, U, A, R> SerCollector<T, A, R> mapping(SerFunction<? super T, ? extends U> mapper, SerCollector<? super U, A, R> downstream)
    {
        SerBiConsumer<A, ? super U> downstreamAccumulator = downstream.accumulator();
        return new SerCollector<>(downstream.supplier(), (r, t) -> downstreamAccumulator.accept(r, mapper.apply(t)),
        		downstream.combiner(), downstream.finisher(), downstream.characteristics());
    }

    public static <T> SerCollector<T, ?, BigDecimal> summingBigDecimal(SerFunction<? super T, BigDecimal> mapper)
    {
        return collectingAndThen(mapping(mapper::apply, reducing(BigDecimal::add)), opt -> (BigDecimal) opt.orElse(BigDecimal.valueOf(0)));
    }

    public static <T> SerCollector<T, double[], Double> summingDouble(SerToDoubleFunction<? super T> mapper)
    {
        return new SerCollector<>(
                () -> new double[3],
                (a, t) -> { sumWithCompensation(a, mapper.applyAsDouble(t));
                            a[2] += mapper.applyAsDouble(t);},
                (a, b) -> { sumWithCompensation(a, b[0]);
                            a[2] += b[2];
                            return sumWithCompensation(a, b[1]); },
                a -> computeFinalSum(a), emptySet());
    }

    public static <T> SerCollector<T, long[], Long> summingLong(SerToLongFunction<? super T> mapper)
    {
        return new SerCollector<>(
                () -> new long[1],
                (a, t) -> { a[0] += mapper.applyAsLong(t); },
                (a, b) -> { a[0] += b[0]; return a; },
                a -> a[0], emptySet());
    }

    public static <T> SerCollector<T, double[], Double> averagingDouble(SerToDoubleFunction<? super T> mapper) 
    {
        return new SerCollector<>(() -> new double[4],
                (a, t) -> { sumWithCompensation(a, mapper.applyAsDouble(t)); a[2]++; a[3]+= mapper.applyAsDouble(t);},
                (a, b) -> { sumWithCompensation(a, b[0]); sumWithCompensation(a, b[1]); a[2] += b[2]; a[3] += b[3]; return a; },
                a -> (a[2] == 0) ? 0.0d : (computeFinalSum(a) / a[2]), emptySet());
    }

    public static <T extends Serializable> SerCollector<T, ?, BigDecimal> averagingBigDecimal(SerFunction<? super T, BigDecimal> mapper)
    {
		return null;//mapping(mapper, teeing(collectingAndThen(reducing(BigDecimal::add), v -> v.orElse(BigDecimal.valueOf(0))), counting(), (sum, n) -> sum.divide(BigDecimal.valueOf(n))));
    }

    public static <T> SerCollector<T, List<T>, List<T>> toList()
    {
        return new SerCollector<>(ArrayList::new, List::add, (left, right) -> { left.addAll(right); return left; }, identity(), emptySet());
    }

	public static <T, A, R> SerCollector<T, A, R> filtering(SerPredicate<? super T> predicate, SerCollector<? super T, A, R> downstream)
	{
		final SerBiConsumer<A, T> biConsumer = (r, t) -> {
			if (predicate.test(t))
				downstream.accumulator().accept(r, t);
		};
		
		return new SerCollector<>(downstream.supplier(), biConsumer, downstream.combiner(), downstream.finisher(), downstream.characteristics());
	}

	public static <T, A, R> SerCollector<T, A, R> peeking(SerConsumer<? super T> action, SerCollector<? super T, A, R> downstream)
	{
		final SerBiConsumer<A, T> biConsumer = (r, t) -> {
			action.accept(t);
			downstream.accumulator().accept(r, t);
		};
		
		return new SerCollector<>(downstream.supplier(), biConsumer, downstream.combiner(), downstream.finisher(), downstream.characteristics());
	}

    public static <T extends Serializable> SerCollector<T, ? extends SerConsumer<T>, Optional<T>> minBy(Comparator<? super T> comparator)
    {
        return reducing(SerBinaryOperator.minBy(comparator));
    }

    public static <T extends Serializable> SerCollector<T, ?, Optional<T>> maxBy(Comparator<? super T> comparator)
    {
        return reducing(SerBinaryOperator.maxBy(comparator));
    }

    public static <T, K> SerCollector<T, ?, ConcurrentMap<K, List<T>>> groupingByConcurrent(SerFunction<? super T, ? extends K> classifier)
    {
        return groupingByConcurrent(classifier, ConcurrentHashMap::new, toList());
    }

    public static <T, K, A, D> SerCollector<T, ?, ConcurrentMap<K, D>> groupingByConcurrent(SerFunction<? super T, ? extends K> classifier, SerCollector<? super T, A, D> downstream)
    {
        return groupingByConcurrent(classifier, ConcurrentHashMap::new, downstream);
    }

    public static <T, K, A, D, M extends ConcurrentMap<K, D>> SerCollector<T, ?, M> groupingByConcurrent(SerFunction<? super T, ? extends K> classifier,
    		SerSupplier<M> mapFactory, SerCollector<? super T, A, D> downstream)
    {
        SerSupplier<A> downstreamSupplier = downstream.supplier();
        SerBiConsumer<A, ? super T> downstreamAccumulator = downstream.accumulator();
        SerBinaryOperator<ConcurrentMap<K, A>> merger = mapMerger(downstream.combiner());
        @SuppressWarnings("unchecked")
        SerSupplier<ConcurrentMap<K, A>> mangledFactory = (SerSupplier<ConcurrentMap<K, A>>) mapFactory;
        SerBiConsumer<ConcurrentMap<K, A>, T> accumulator;
        if (downstream.characteristics().contains(CONCURRENT))
            accumulator = (m, t) -> {
                K key = Objects.requireNonNull(classifier.apply(t), "element cannot be mapped to a null key");
                A resultContainer = m.computeIfAbsent(key, k -> downstreamSupplier.get());
                downstreamAccumulator.accept(resultContainer, t);
            };
        else
            accumulator = (m, t) -> {
                K key = Objects.requireNonNull(classifier.apply(t), "element cannot be mapped to a null key");
                A resultContainer = m.computeIfAbsent(key, k -> downstreamSupplier.get());
                synchronized (resultContainer) {
                    downstreamAccumulator.accept(resultContainer, t);
                }
            };

        if (downstream.characteristics().contains(IDENTITY_FINISH))
        {
            @SuppressWarnings("unchecked")
            SerFunction<ConcurrentMap<K, A>, M> downstreamFinisher = (SerFunction<ConcurrentMap<K, A>, M>) downstream.finisher();
            return SerCollector.of(mangledFactory, accumulator, merger, downstreamFinisher, EnumSet.of(CONCURRENT, UNORDERED, IDENTITY_FINISH));
        }
        else
        {
            @SuppressWarnings("unchecked")
            SerFunction<A, A> downstreamFinisher = (SerFunction<A, A>) downstream.finisher();
            SerFunction<ConcurrentMap<K, A>, M> finisher = intermediate -> {
                intermediate.replaceAll((k, v) -> downstreamFinisher.apply(v));
                @SuppressWarnings("unchecked")
                M castResult = (M) intermediate;
                return castResult;
            };
            
            return SerCollector.of(mangledFactory, accumulator, merger, finisher, EnumSet.of(CONCURRENT, UNORDERED));
        }
    }
    
    public static <T extends Serializable> SerCollector<T, OptionalBox<T>, Optional<T>> reducing(SerBinaryOperator<T> op)
    {
        return new SerCollector<>(() -> new OptionalBox<T>(op), OptionalBox::accept,
                (a, b) -> { if (b.isPresent()) a.accept(b.get()); return a; },
                a -> Optional.ofNullable(a.get()), emptySet());
    }

    public static <T, A1, A2, R1, R2, R> SerCollector<T, PairBox<T, A1, A2, R1, R2, R>, R> teeing(SerCollector<? super T, A1, R1> downstream1, 
    		SerCollector<? super T, A2, R2> downstream2, SerBiFunction<R1, R2, R> merger) 
    {
        EnumSet<Characteristics> characteristics = EnumSet.noneOf(Characteristics.class);
        characteristics.addAll(downstream1.characteristics());
        characteristics.retainAll(downstream2.characteristics());
        characteristics.remove(IDENTITY_FINISH);

        return SerCollector.of(() -> new PairBox<>(downstream1, downstream2, merger), PairBox::accumulate, PairBox::combine, PairBox::finish, characteristics);
    }

    static class PairBox<T, A1, A2, R1, R2, R>
    {
        A1 a1;
        A2 a2;
		private final SerCollector<? super T, A1, R1> downstream1;
		private final SerCollector<? super T, A2, R2> downstream2;
		private final SerBiFunction<? super R1, ? super R2, R> merger;

        public PairBox(SerCollector<? super T, A1, R1> downstream1, SerCollector<? super T, A2, R2> downstream2, SerBiFunction<? super R1, ? super R2, R> merger)
		{
            this.downstream1 = downstream1;
			this.downstream2 = downstream2;
			this.merger = merger;
			a1 = downstream1.supplier().get();
            a2 = downstream2.supplier().get();
		}

        void accumulate(T t)
        {
        	downstream1.accumulator().accept(a1, t);
        	downstream2.accumulator().accept(a2, t);
        }

        PairBox<T, A1, A2, R1, R2, R> combine(PairBox<T, A1, A2, R1, R2, R> other)
        {
            a1 = downstream1.combiner().apply(a1, other.a1);
            a2 = downstream2.combiner().apply(a2, other.a2);
            return this;
        }

        R finish()
        {
            R1 r1 = downstream1.finisher().apply(a1);
            R2 r2 = downstream2.finisher().apply(a2);
            return merger.apply(r1, r2);
        }
    }

    private static double[] sumWithCompensation(double[] intermediateSum, double value)
    {
        double tmp = value - intermediateSum[1];
        double sum = intermediateSum[0];
        double velvel = sum + tmp; // Little wolf of rounding error
        intermediateSum[1] = (velvel - sum) - tmp;
        intermediateSum[0] = velvel;
        return intermediateSum;
    }

    private static double computeFinalSum(double[] summands) 
    {
        double tmp = summands[0] + summands[1];
        double simpleSum = summands[summands.length - 1];
        if (Double.isNaN(tmp) && Double.isInfinite(simpleSum))
            return simpleSum;
        else
            return tmp;
    }

    @SuppressWarnings("unchecked")
    private static <T> SerSupplier<T[]> boxSupplier(T identity) {
        return () -> (T[]) new Object[] { identity };
    }

    private static <K, V, M extends Map<K,V>> SerBinaryOperator<M> mapMerger(SerBinaryOperator<V> mergeFunction)
    {
        return (m1, m2) -> {
            for (Map.Entry<K,V> e : m2.entrySet())
                m1.merge(e.getKey(), e.getValue(), mergeFunction);
            return m1;
        };
    }

    private static <T> SerBinaryOperator<T> throwingMerger()
    {
        return (u, v) -> { 
        	throw new IllegalStateException(String.format("Duplicate key %s", u)); 
        };
    }

	public static <U, V, R> SerCollector<Entry<U, V>, ?, ConcurrentMap<R, V>> mappingKeys(SerFunction<? super U, ? extends R> keyMapper)
	{
		return toConcurrentMap(keyMapper.compose(Entry::getKey), Entry::getValue);
	}

	public static <U, V> SerCollector<Entry<U, V>, ?, ConcurrentMap<U, V>> mappingConcurrentEntries(SerSupplier<ConcurrentMap<U, V>> factory)
	{
		return toConcurrentMap(Entry::getKey, Entry::getValue, (a, b) -> {
			throw new UnsupportedOperationException("Duplicate");
		}, factory);
	}

	public static <U, V, R> SerCollector<Entry<U, V>, ?, ConcurrentMap<U, R>> groupingByKeys(SerCollector<Entry<U, V>, ?, R> valueMapper)
	{
		return groupingByConcurrent(Entry::getKey, valueMapper);
	}

	public static <U, V, R> SerCollector<Entry<U, V>, ?, ConcurrentMap<U, R>> mappingValues(SerFunction<? super V, ? extends R> valueMapper)
	{
		return toConcurrentMap(Entry::getKey, valueMapper.compose(Entry::getValue));
	}

	public static <U, V> SerCollector<Entry<U, V>, ?, ConcurrentMap<U, V>> entriesToMap(SerBinaryOperator<V> combiner)
	{
		return toConcurrentMap(Entry::getKey, Entry::getValue, combiner);
	}

	public static <K, V> SerCollector<V, ?, ConcurrentMap<K, V>> toMapWithKeys(SerFunction<? super V, ? extends K> valueToKeyMapper)
	{
		return toConcurrentMap(valueToKeyMapper, identity());
	}

	public static <K, V> SerCollector<K, ?, ConcurrentMap<K, V>> toMapWithValues(SerFunction<K, V> keyToValueMapper)
	{
		return toConcurrentMap(identity(), keyToValueMapper);
	}

	public static <U, V> SerCollector<Entry<? extends U, ? extends V>, ?, ConcurrentMap<U, V>> entriesToMap()
	{
		return toConcurrentMap(Entry::getKey, Entry::getValue);
	}
}
