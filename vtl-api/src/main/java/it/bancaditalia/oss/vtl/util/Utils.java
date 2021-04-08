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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.toConcurrentMap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class contains various utility functions used by the VTL Engine implementation.
 * Most of the functions are wrappers of the standard packages in Java 8.
 * 
 * @see java.util.function
 * @see java.util.stream
 * @see java.util
 * 
 * @author Valentino Pinna
 */
public final class Utils
{
	public static final boolean SEQUENTIAL = "true".equalsIgnoreCase(System.getProperty("vtl.sequential"));
	public static final boolean ORDERED = "true".equalsIgnoreCase(System.getProperty("vtl.ordered"));
	
	private Utils() {}

	public static <K, V1, V2> Function<Entry<K, V1>, Entry<K, V2>> keepingKey(Function<? super V1, ? extends V2> valueMapper)
	{
		return e -> new SimpleEntry<>(e.getKey(), valueMapper.apply(e.getValue()));
	}

	public static <K, V> Consumer<Entry<K, V>> atKey(Consumer<? super K> valueMapper)
	{
		return e -> valueMapper.accept(e.getKey());
	}

	public static <K, V> Consumer<Entry<K, V>> atValue(Consumer<? super V> valueMapper)
	{
		return e -> valueMapper.accept(e.getValue());
	}

	public static <R> UnaryOperator<R> onlyIf(Predicate<? super R> condition, UnaryOperator<R> mapper)
	{
		return v -> condition.test(v) ? mapper.apply(v) : v;
	}

	public static <T, R> Predicate<T> afterMapping(Function<? super T, ? extends R> mapper, Predicate<? super R> condition)
	{
		return t -> condition.test(mapper.apply(t));
	}
	
	public static <K1, K2, V> Function<Entry<K1, V>, Entry<K2, V>> keepingValue(Function<? super K1, ? extends K2> keyMapper)
	{
		return e -> new SimpleEntry<>(keyMapper.apply(e.getKey()), e.getValue());
	}

	public static <K, A, B> Function<Entry<K, A>, Entry<K, B>> keepingKey(BiFunction<K, A, B> valueMapper)
	{
		return e -> new SimpleEntry<>(e.getKey(), valueMapper.apply(e.getKey(), e.getValue()));
	}

	public static <T, K, V> Function<T, Entry<K, V>> toEntry(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends V> valueMapper)
	{
		return t -> new SimpleEntry<>(keyMapper.apply(t), valueMapper.apply(t));
	}

	public static <K, V> Function<K, Entry<K, V>> toEntryWithValue(Function<? super K, ? extends V> keyToValueMapper)
	{
		return k -> new SimpleEntry<>(k, keyToValueMapper.apply(k));
	}

	public static <K, V> Function<V, Entry<K, V>> toEntryWithKey(Function<? super V, ? extends K> valueToKeyMapper)
	{
		return v -> new SimpleEntry<>(valueToKeyMapper.apply(v), v);
	}

	public static <U, V> Collector<Entry<? extends U, ? extends V>, ?, ConcurrentMap<U, V>> entriesToMap()
	{
		return toConcurrentMap(Entry::getKey, Entry::getValue);
	}

	public static <K, V> Collector<K, ?, ConcurrentMap<K, V>> toMapWithValues(Function<? super K, ? extends V> valueMapper)
	{
		return toConcurrentMap(identity(), valueMapper);
	}

	public static <K, V> Collector<V, ?, ConcurrentMap<K, V>> toMapWithKeys(Function<? super V, ? extends K> valueMapper)
	{
		return toConcurrentMap(valueMapper, identity());
	}

	public static <U, V, M extends ConcurrentMap<U, V>> Collector<Entry<U, V>, ?, M> entriesToMap(Supplier<M> mapSupplier)
	{
		return toConcurrentMap(Entry::getKey, Entry::getValue, (a, b) -> a, mapSupplier);
	}

	public static <U, V> Collector<Entry<U, V>, ?, ConcurrentMap<U, V>> entriesToMap(BinaryOperator<V> combiner)
	{
		return toConcurrentMap(Entry::getKey, Entry::getValue, combiner);
	}

	public static <U, V, R> Collector<Entry<U, V>, ?, ConcurrentMap<U, R>> mappingValues(Function<? super V, ? extends R> valueMapper)
	{
		return toConcurrentMap(Entry::getKey, valueMapper.compose(Entry::getValue));
	}

	public static <U, V, R> Collector<Entry<U, V>, ?, ConcurrentMap<R, V>> mappingKeys(Function<? super U, ? extends R> keyMapper)
	{
		return toConcurrentMap(keyMapper.compose(Entry::getKey), Entry::getValue);
	}

	public static <U, V> Collector<Entry<U, V>, ?, ConcurrentMap<U, V>> mappingConcurrentEntries(Supplier<ConcurrentMap<U, V>> factory)
	{
		return toConcurrentMap(Entry::getKey, Entry::getValue, (a, b) -> {
			throw new UnsupportedOperationException("Duplicate");
		}, factory);
	}

	public static <U, V, R> Collector<Entry<U, V>, ?, ConcurrentMap<U, R>> groupingByKeys(Collector<Entry<U, V>, ?, R> valueMapper)
	{
		return groupingByConcurrent(Entry::getKey, valueMapper);
	}

	public static <T, R> R folding(R accumulator, Stream<T> stream, BiFunction<? super R, ? super T, ? extends R> aggregator)
	{
		for (T object : (Iterable<T>) () -> stream.iterator())
			accumulator = aggregator.apply(accumulator, object);
		return accumulator;
	}

	public static <T, R> R fold(R accumulator, Iterable<T> iterable, BiFunction<? super R, ? super T, ? extends R> aggregator)
	{
		for (T object : iterable)
			accumulator = aggregator.apply(accumulator, object);

		return accumulator;
	}

	public static <K, V> Predicate<Entry<K, V>> entryByValue(Predicate<V> valuePredicate)
	{
		return e -> valuePredicate.test(e.getValue());
	}

	public static <K, V> Predicate<Entry<K, V>> entryByKey(Predicate<K> keyPredicate)
	{
		return e -> keyPredicate.test(e.getKey());
	}

	public static <K, V> Predicate<? super Entry<K, V>> entryByKeyValue(BiPredicate<? super K, ? super V> entryPredicate)
	{
		return e -> entryPredicate.test(e.getKey(), e.getValue());
	}

	public static <K, V, R> Function<Entry<K, V>, R> splitting(BiFunction<? super K, ? super V, ? extends R> function)
	{
		return e -> function.apply(e.getKey(), e.getValue());
	}

	public static <K, V> Consumer<Entry<K, V>> splittingConsumer(BiConsumer<? super K, ? super V> function)
	{
		return e -> function.accept(e.getKey(), e.getValue());
	}

	public static <A extends Serializable, B extends Serializable, C extends Serializable, R> Function<Triple<A, B, C>, R> splitting(TriFunction<? super A, ? super B, ? super C, ? extends R> trifunction)
	{
		return t -> trifunction.apply(t.getFirst(), t.getSecond(), t.getThird());
	}

	public static <A extends Serializable, B extends Serializable, C extends Serializable> Consumer<Triple<A, B, C>> triple(TriConsumer<? super A, ? super B, ? super C> triconsumer)
	{
		return t -> triconsumer.accept(t.getFirst(), t.getSecond(), t.getThird());
	}

	public static <A extends Serializable, B extends Serializable, C extends Serializable, D extends Serializable> Consumer<Quadruple<A, B, C, D>> quartet(QuadConsumer<? super A, ? super B, ? super C, ? super D> quadconsumer)
	{
		return q -> quadconsumer.accept(q.getFirst(), q.getSecond(), q.getThird(), q.getFourth());
	}

	public static <A extends Serializable, B extends Serializable, C extends Serializable, D extends Serializable, R> Function<Quadruple<A, B, C, D>, R> splitting(QuadFunction<? super A, ? super B, ? super C, ? super D, ? extends R> quadfunction)
	{
		return q -> quadfunction.apply(q.getFirst(), q.getSecond(), q.getThird(), q.getFourth());
	}

	public static <T> Stream<T> getStream(Spliterator<T> source)
	{
		Stream<T> stream = StreamSupport.stream(source, !SEQUENTIAL);
		return ORDERED ? stream : stream.unordered();
	}

	public static <K, V> Stream<Entry<K, V>> getStream(Map<K, V> source)
	{
		Stream<Entry<K, V>> stream = SEQUENTIAL ? source.entrySet().stream() : source.entrySet().parallelStream();
		return (ORDERED ? stream : stream.unordered());
	}

	public static <T> Stream<T> getStream(Collection<T> source)
	{
		Stream<T> stream = SEQUENTIAL ? source.stream() : source.parallelStream();
		return (ORDERED ? stream : stream.unordered());
	}

	public static <T> Stream<T> getStream(Stream<T> stream)
	{
		stream = SEQUENTIAL ? stream.sequential() : stream.parallel();
		return ORDERED ? stream : stream.unordered();
	}

	public static IntStream getStream(int max)
	{
		IntStream stream = IntStream.range(0, max);
		stream = SEQUENTIAL ? stream.sequential() : stream.parallel();
		return ORDERED ? stream : stream.unordered();
	}

	public static Stream<String> getStream(String fileName) throws IOException
	{
		@SuppressWarnings("resource")
		Stream<String> stream = Files.lines(Paths.get(fileName), UTF_8);
		stream = SEQUENTIAL ? stream.sequential() : stream.parallel();
		return ORDERED ? stream : stream.unordered();
	}

	@SafeVarargs
	public static <T, K extends T> Stream<T> getStream(K... elements)
	{
		Stream<T> stream = Stream.of(elements);
		stream = SEQUENTIAL ? stream.sequential() : stream.parallel();
		return ORDERED ? stream : stream.unordered();
	}

	public static IntStream getStream(int[] is)
	{
		IntStream stream = is != null ? Arrays.stream(is) : IntStream.empty();
		stream = SEQUENTIAL ? stream.sequential() : stream.parallel();
		return ORDERED ? stream : stream.unordered();
	}

	public static DoubleStream getStream(double[] ds)
	{
		DoubleStream stream = ds != null ? Arrays.stream(ds) : DoubleStream.empty();
		stream = SEQUENTIAL ? stream.sequential() : stream.parallel();
		return ORDERED ? stream : stream.unordered();
	}

	public static <T, U, A, R> Collector<T, ?, R> flatMapping(Function<? super T, ? extends Stream<? extends U>> mapper, Collector<? super U, A, R> downstream)
	{
		final BiConsumer<A, T> biConsumer = (r, t) -> 
			Optional.ofNullable(mapper.apply(t)).ifPresent(s -> s.forEach(u -> downstream.accumulator().accept(r, u)));

		return Collector.of(downstream.supplier(), biConsumer, downstream.combiner(), downstream.finisher(),
				downstream.characteristics().toArray(new Characteristics[0]));
	}

	public static <T, A, R> Collector<T, ?, R> filtering(Predicate<? super T> predicate, Collector<? super T, A, R> downstream)
	{
		final BiConsumer<A, T> biConsumer = (r, t) -> {
			if (predicate.test(t))
				downstream.accumulator().accept(r, t);
		};
		
		return Collector.of(downstream.supplier(), biConsumer, downstream.combiner(), downstream.finisher(),
				downstream.characteristics().toArray(new Characteristics[0]));
	}

	public static <T> Predicate<T> not(Predicate<T> target) 
	{
        return target.negate();
    }

	public static <A1 extends Serializable, A2 extends Serializable, B extends Serializable, C extends Serializable> Function<Triple<A1, B, C>, Triple<A2, B, C>> changingFirst(Function<? super A1, ? extends A2> mapper)
	{
		return t -> new Triple<>(mapper.apply(t.getFirst()), t.getSecond(), t.getThird());
	}
	
	public static <T> UnaryOperator<Set<T>> retainer(Set<? extends T> toRetain)
	{
		return oldSet -> {
			Set<T> newSet = new HashSet<>(oldSet);
			newSet.retainAll(toRetain);
			return newSet;
		};
	}

	public static int[] catArrays(int[] a, int[] b) 
	{
	    int[] c = new int[a.length + b.length];
	    System.arraycopy(a, 0, c, 0, a.length);
	    System.arraycopy(b, 0, c, a.length, b.length);
	    return c;
	}

	public static <K, V> Map<K, V> retainOnly(Map<? extends K, ? extends V> originalMap, Set<? extends K> askedKeys)
	{
		Map<K, V> classifierMap = new HashMap<>(originalMap);
		classifierMap.keySet().retainAll(askedKeys);
		return classifierMap;
	}

	public static <A, B, C> Function<A, Function<B, C>> curry(BiFunction<A, B, C> bifunction)
	{
		return a -> b -> bifunction.apply(a, b);
	}

	public static <A, B> Function<A, Consumer<B>> curry(BiConsumer<A, B> biconsumer)
	{
		return a -> b -> biconsumer.accept(a, b);
	}

	public static <A, B, C> Function<B, Function<A, C>> curryRev(BiFunction<A, B, C> bifunction)
	{
		return b -> a -> bifunction.apply(a, b);
	}
	
	public static <A, B> Function<A, B> asFun(Function<? super A, ? extends B> function)
	{
		return a -> function.apply(a);
	}
	
	public static <A> BinaryOperator<A> reverseIfBOp(boolean isReverse, BinaryOperator<A> binaryOperator)
	{
		return (a, b) -> isReverse ? binaryOperator.apply(b, a) : binaryOperator.apply(a, b);
	}

	public static <A, R> BiFunction<A, A, R> reverseIf(boolean isReverse, BiFunction<? super A, ? super A, ? extends R> bifunction)
	{
		return (a, b) -> isReverse ? bifunction.apply(b, a) : bifunction.apply(a, b);
	}

	public static <A, B, C> BiFunction<A, B, C> asFun(BiFunction<? super A, ? super B, ? extends C> bifunction)
	{
		return (a, b) -> bifunction.apply(a, b);
	}
	
	@SuppressWarnings("ReturnValueIgnored")
	public <T> Consumer<T> asConsumer(Function<? super T, ?> function)
	{
		return function::apply;
	}

	public static <T, U> BiPredicate<T, U> not(BiPredicate<T, U> test)
	{
		return (a, b) -> !test.test(a, b);
	}
	
	public static <T, C extends Collection<T>> C coalesce(C value, C defaultValue)
	{
		return value != null ? value : defaultValue;
	}
	
	public static <T> T coalesce(T value, T defaultValue)
	{
		return value != null ? value : defaultValue;
	}

	public static <T> T coalesceSwapped(T defaultValue, T value)
	{
		return value != null ? value : defaultValue;
	}
	
	@SafeVarargs
	public static <T> Set<? extends T> setOf(T... elements)
	{
		Set<T> result = new HashSet<>();
		for (T elem: elements)
			result.add(elem);
		return Collections.unmodifiableSet(result);
	}
}
