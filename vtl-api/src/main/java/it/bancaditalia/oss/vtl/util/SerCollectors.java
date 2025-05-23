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
import static it.bancaditalia.oss.vtl.util.Utils.splittingConsumer;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collector.Characteristics;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;

public class SerCollectors
{
    public static <T, K, U> SerCollector<T, ?, ConcurrentMap<K, U>> toConcurrentMap(SerFunction<? super T, ? extends K> keyMapper,
            SerFunction<? super T, ? extends U> valueMapper, SerBinaryOperator<U> mergeFunction)
	{
		return toConcurrentMap(keyMapper, valueMapper, mergeFunction, ConcurrentHashMap::new);
	}

	public static <T, K, U, M extends ConcurrentMap<K, U>> SerCollector<T, ?, M> toConcurrentMap(SerFunction<? super T, ? extends K> kMapper, 
			SerFunction<? super T, ? extends U> vMapper, SerBinaryOperator<U> mergeFun, SerSupplier<M> mapSupplier)
	{
		SerBiConsumer<M, T> acc = (map, e) -> map.merge(kMapper.apply(e), vMapper.apply(e), mergeFun);

		return SerCollector.of(mapSupplier, acc, mapMerger(mergeFun), identity(), EnumSet.of(CONCURRENT, UNORDERED, IDENTITY_FINISH));
	}

	public static <T, K, U, M extends ConcurrentMap<K, U>> SerCollector<T, ?, M> toConcurrentMap(SerFunction<? super T, ? extends K> kMapper, 
			SerFunction<? super T, ? extends U> vMapper, SerSupplier<M> mapSupplier)
	{
		return SerCollector.of(mapSupplier, throwingPutter(kMapper, vMapper), throwingMerger(), identity(), EnumSet.of(CONCURRENT, UNORDERED, IDENTITY_FINISH));
	}

    public static <T, K, U> SerCollector<T, ?, ConcurrentMap<K, U>> toConcurrentMap(SerFunction<? super T, 
    		? extends K> keyMapper, SerFunction<? super T, ? extends U> valueMapper)
    {
        return SerCollector.of(ConcurrentHashMap::new, throwingPutter(keyMapper, valueMapper), (a, b) -> a, EnumSet.of(CONCURRENT, UNORDERED, IDENTITY_FINISH));
    }

    public static <T, K, U> SerCollector<T, ?, Map<K, U>> toMap(SerFunction<? super T, ? extends K> kMapper, SerFunction<? super T, ? extends U> vMapper)
    {
        return SerCollector.of(HashMap::new, throwingPutter(kMapper, vMapper), throwingMerger(), identity(), EnumSet.of(UNORDERED, IDENTITY_FINISH));
    }

    public static <T, K, U, M extends Map<K, U> & Serializable> SerCollector<T, ?, M> toMap(SerFunction<? super T, ? extends K> kMapper,
    		SerFunction<? super T, ? extends U> vMapper, SerSupplier<M> mapSupplier)
    {
        return SerCollector.of(mapSupplier, throwingPutter(kMapper, vMapper), throwingMerger(),  identity(), EnumSet.of(UNORDERED, IDENTITY_FINISH));
    }

    public static <T, A, R, RR> SerCollector<T, A, RR> collectingAndThen(SerCollector<T, A, ? extends R> downstream, SerFunction<? super R, RR> finisher)
	{
		EnumSet<Characteristics> characteristics = EnumSet.copyOf(downstream.characteristics());
		characteristics.remove(IDENTITY_FINISH);
		return SerCollector.of(downstream.supplier(), downstream.accumulator(), downstream.combiner(), downstream.finisher().andThen(finisher), characteristics);
	}
	
    public static <T> SerCollector<T, ?, Set<T>> toSet()
    {
        return SerCollector.of(HashSet::new, Set::add, (left, right) -> { left.addAll(right); return left; }, identity(), EnumSet.of(UNORDERED, IDENTITY_FINISH));
    }

    public static <T, C extends Collection<T>> SerCollector<T, C, C> toCollection(SerSupplier<C> collectionFactory)
    {
        return SerCollector.of(collectionFactory, Collection::add, (left, right) -> { left.addAll(right); return left; }, identity(), EnumSet.of(IDENTITY_FINISH));
    }

    public static <T> SerCollector<T, ?, Long> counting()
    {
        return SerCollector.of(AtomicLong::new, (l, v) -> l.incrementAndGet(), (l, r) -> l, AtomicLong::get, EnumSet.of(UNORDERED, CONCURRENT));
    }

    public static <T, U, A, R> SerCollector<T, ?, R> mapping(SerFunction<? super T, ? extends U> mapper, SerCollector<? super U, A, R> downstream)
    {
        SerBiConsumer<A, ? super U> downstreamAccumulator = downstream.accumulator();
        return SerCollector.of(downstream.supplier(), (r, t) -> downstreamAccumulator.accept(r, mapper.apply(t)),
        		downstream.combiner(), downstream.finisher(), downstream.characteristics());
    }

    public static <T> SerCollector<T, ?, List<T>> toList()
    {
    	return toList(ArrayList::new);
    }

    public static <T, L extends List<T>> SerCollector<T, ?, L> toList(SerSupplier<? extends L> supplier)
    {
        return SerCollector.of(supplier::get, List::add, (left, right) -> { left.addAll(right); return left; }, identity(), EnumSet.of(IDENTITY_FINISH));
    }

    public static <T> SerCollector<T, ?, T[]> toArray(T[] result)
    {
    	AtomicInteger index = new AtomicInteger(0);
        return SerCollector.of(() -> result, (a, v) -> a[index.getAndIncrement()] = v, (a, b) -> a, identity(), EnumSet.of(CONCURRENT, IDENTITY_FINISH));
    }

	public static <T, A, R> SerCollector<T, A, R> filtering(SerPredicate<? super T> predicate, SerCollector<? super T, A, R> downstream)
	{
		SerBiConsumer<A, T> biConsumer = (r, t) -> {
				if (predicate.test(t))
					downstream.accumulator().accept(r, t);
			};
		
		return SerCollector.of(downstream.supplier(), biConsumer, downstream.combiner(), downstream.finisher(), downstream.characteristics());
	}

	public static <T, A, R> SerCollector<T, A, R> peeking(SerConsumer<? super T> action, SerCollector<? super T, A, R> downstream)
	{
		final SerBiConsumer<A, T> biConsumer = (r, t) -> {
			action.accept(t);
			downstream.accumulator().accept(r, t);
		};
		
		return SerCollector.of(downstream.supplier(), biConsumer, downstream.combiner(), downstream.finisher(), downstream.characteristics());
	}

    public static <T, C extends Comparator<? super T> & Serializable> SerCollector<T, ?, Optional<T>> minBy(Class<?> repr, C comparator)
    {
        return reducing(repr, SerBinaryOperator.minBy(comparator));
    }

    public static <T, C extends Comparator<? super T> & Serializable> SerCollector<T, ?, Optional<T>> maxBy(Class<?> repr, C comparator)
    {
        return reducing(repr, SerBinaryOperator.maxBy(comparator));
    }
    
    public static <T> SerCollector<T, ?, Optional<T>> reducing(Class<?> repr, SerBinaryOperator<T> op)
    {
    	return SerCollector.of(() -> new Holder<T>(repr), (r, v) -> r.accumulateAndGet(v, (v1, v2) -> v1 == null ? v2 : op.apply(v1, v2)), (l, r) -> l, r -> Optional.ofNullable(r.get()), EnumSet.of(CONCURRENT));
    }

    public static <T> SerCollector<T, ?, Optional<T>> firstValue(Class<?> repr)
    {
    	@SuppressWarnings("unchecked")
		T nullHolder = (T) new Object[0];
    	return SerCollector.of(() -> {
    		Holder<T> holder = new Holder<T>(repr);
    		holder.set(nullHolder);
    		return holder;
    	}, (holder, v) -> holder.compareAndSet(nullHolder, v), (holderA, holderB) -> holderA, r -> { 
    		T value = r.get();
			return Optional.ofNullable(value == nullHolder ? null : value);
    	}, EnumSet.of(CONCURRENT));
    }
    
    public static <T> SerCollector<T, ?, Optional<T>> lastValue(Class<?> repr)
    {
    	return SerCollector.of(() -> new Holder<T>(repr), Holder::set, (a, b) -> b, r -> Optional.ofNullable(r.get()), EnumSet.of(CONCURRENT));
    }
    
    public static <T, K> SerCollector<T, ?, ConcurrentMap<K, List<T>>> groupingByConcurrent(SerFunction<? super T, ? extends K> classifier)
    {
        return groupingByConcurrent(classifier, ConcurrentHashMap::new, toList());
    }

    public static <T, K, D> SerCollector<T, ?, ConcurrentMap<K, D>> groupingByConcurrent(SerFunction<? super T, ? extends K> classifier, SerCollector<? super T, ?, D> downstream)
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
    
    public static <T, K, A, D, M extends Map<K, D>> SerCollector<T, ?, M> groupingBy(SerFunction<? super T, ? extends K> classifier,
    		SerSupplier<? extends M> mapFactory, SerCollector<? super T, A, D> downstream)
    {
        SerSupplier<A> downstreamSupplier = downstream.supplier();
        SerBiConsumer<A, ? super T> downstreamAccumulator = downstream.accumulator();
        SerBinaryOperator<Map<K, A>> merger = mapMerger(downstream.combiner());
        @SuppressWarnings("unchecked")
        SerSupplier<Map<K, A>> mangledFactory = (SerSupplier<Map<K, A>>) mapFactory;
        SerBiConsumer<Map<K, A>, T> accumulator;
        accumulator = (m, t) -> {
            K key = Objects.requireNonNull(classifier.apply(t), "element cannot be mapped to a null key");
            A resultContainer = m.computeIfAbsent(key, k -> downstreamSupplier.get());
            downstreamAccumulator.accept(resultContainer, t);
        };

        if (downstream.characteristics().contains(IDENTITY_FINISH))
        {
            @SuppressWarnings("unchecked")
            SerFunction<Map<K, A>, M> downstreamFinisher = (SerFunction<Map<K, A>, M>) downstream.finisher();
            return SerCollector.of(mangledFactory, accumulator, merger, downstreamFinisher, EnumSet.of(UNORDERED, IDENTITY_FINISH));
        }
        else
        {
            @SuppressWarnings("unchecked")
            SerFunction<A, A> downstreamFinisher = (SerFunction<A, A>) downstream.finisher();
            SerFunction<Map<K, A>, M> finisher = intermediate -> {
                intermediate.replaceAll((k, v) -> downstreamFinisher.apply(v));
                @SuppressWarnings("unchecked")
                M castResult = (M) intermediate;
                return castResult;
            };
            
            return SerCollector.of(mangledFactory, accumulator, merger, finisher, EnumSet.of(UNORDERED));
        }
    }

    public static <T, R1, R2, R> SerCollector<T, ?, R> teeing(SerCollector<? super T, ?, R1> downstream1, 
    		SerCollector<? super T, ?, R2> downstream2, SerBiFunction<? super R1, ? super R2, R> merger) 
    {
        EnumSet<Characteristics> characteristics = EnumSet.noneOf(Characteristics.class);
        if (downstream1.characteristics().contains(CONCURRENT) && downstream2.characteristics().contains(CONCURRENT))
        	characteristics.add(CONCURRENT);
        if (downstream1.characteristics().contains(UNORDERED) && downstream2.characteristics().contains(UNORDERED))
        	characteristics.add(UNORDERED);

        return SerCollector.of(() -> new PairBox<>(downstream1, downstream2, merger), PairBox::accumulate, PairBox::combine, PairBox::finish, characteristics);
    }

    private static class PairBox<T, A1, A2, R1, R2, R> implements Serializable
    {
		private static final long serialVersionUID = 1L;
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

        public void accumulate(T t)
        {
        	downstream1.accumulator().accept(a1, t);
        	downstream2.accumulator().accept(a2, t);
        }

        public PairBox<T, A1, A2, R1, R2, R> combine(PairBox<T, A1, A2, R1, R2, R> other)
        {
            a1 = downstream1.combiner().apply(a1, other.a1);
            a2 = downstream2.combiner().apply(a2, other.a2);
            return this;
        }

        public R finish()
        {
            R1 r1 = downstream1.finisher().apply(a1);
            R2 r2 = downstream2.finisher().apply(a2);
            return merger.apply(r1, r2);
        }
    }

    private static <K, V, M extends Map<K,V>> SerBinaryOperator<M> mapMerger(SerBinaryOperator<V> mergeFunction)
    {
        return (m1, m2) -> {
            for (Map.Entry<K,V> e: m2.entrySet())
            	try
            	{
            		m1.merge(e.getKey(), e.getValue(), mergeFunction);
            	}
            	catch (IllegalStateException ex)
	            {
	            	throw new VTLNestedException("Error merging key " + e.getKey(), ex);
	            }
            return m1;
        };
    }

    private static <T, K, V, M extends Map<K, V>> SerBiConsumer<M, T> throwingPutter(SerFunction<? super T, ? extends K> keyMapper, SerFunction<? super T, ? extends V> valueMapper)
    {
        return (m, t) -> {
    		final K key = keyMapper.apply(t);
			final V value = valueMapper.apply(t);
			if (m.putIfAbsent(requireNonNull(key), requireNonNull(value)) != null)
            	throw new IllegalStateException(String.format("Duplicate key %s with values %s and %s", key, value, m.get(key)));
        };
    }

    private static <K, V, T extends Map<K, V>> SerBinaryOperator<T> throwingMerger()
    {
        return (u, v) -> {
        	v.entrySet().forEach(splittingConsumer((key, value) -> {
				if (u.putIfAbsent(key, requireNonNull(value)) != null)
                	throw new IllegalStateException(String.format("Duplicate key %s with values %s and %s", key, value, u.get(key)));
        	}));
    		return u;
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
	
	public static <K> SerCollector<K, ?, Set<K>> toConcurrentSet()
	{
		return collectingAndThen(toConcurrentMap(identity(), k -> TRUE, (a, b) -> a, () -> new ConcurrentHashMap<>(1000, 1f, 32)), ConcurrentHashMap::keySet);
	}

	public static <U, V> SerCollector<Entry<? extends U, ? extends V>, ?, ConcurrentMap<U, V>> entriesToMap()
	{
		return toConcurrentMap(Entry::getKey, Entry::getValue);
	}

	public static <U, V, M extends Map<U, V> & Serializable> SerCollector<Entry<? extends U, ? extends V>, ?, M> entriesToMap(SerSupplier<M> mapSupplier, EnumSet<Characteristics> characteristics)
	{
        return SerCollector.of(mapSupplier::get, throwingPutter(Entry::getKey, Entry::getValue), throwingMerger(), characteristics);
	}
}
