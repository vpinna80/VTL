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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import it.bancaditalia.oss.vtl.model.data.DataPoint;

/**
 * This class contains various utility SerFunctions used by the VTL Engine implementation.
 * Most of the SerFunctions are wrappers of the standard packages in Java 8.
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

	public static <K, V1, V2> SerFunction<Entry<K, V1>, Entry<K, V2>> keepingKey(SerFunction<? super V1, ? extends V2> valueMapper)
	{
		return e -> new SimpleEntry<>(e.getKey(), valueMapper.apply(e.getValue()));
	}

	public static <K, V> SerConsumer<Entry<K, V>> atKey(SerConsumer<? super K> valueMapper)
	{
		return e -> valueMapper.accept(e.getKey());
	}

	public static <K, V> SerConsumer<Entry<K, V>> atValue(SerConsumer<? super V> valueMapper)
	{
		return e -> valueMapper.accept(e.getValue());
	}

	public static <R> SerUnaryOperator<R> onlyIf(SerPredicate<? super R> condition, SerUnaryOperator<R> mapper)
	{
		return v -> condition.test(v) ? mapper.apply(v) : v;
	}

	public static <T, R> SerPredicate<T> afterMapping(SerFunction<? super T, ? extends R> mapper, SerPredicate<? super R> condition)
	{
		return t -> condition.test(mapper.apply(t));
	}
	
	public static <K1, K2, V> SerFunction<Entry<K1, V>, Entry<K2, V>> keepingValue(SerFunction<? super K1, ? extends K2> keyMapper)
	{
		return e -> new SimpleEntry<>(keyMapper.apply(e.getKey()), e.getValue());
	}

	public static <K, A, B> SerFunction<Entry<K, A>, Entry<K, B>> keepingKey(SerBiFunction<K, A, B> valueMapper)
	{
		return e -> new SimpleEntry<>(e.getKey(), valueMapper.apply(e.getKey(), e.getValue()));
	}

	public static <T, K, V> SerFunction<T, Entry<K, V>> toEntry(SerFunction<? super T, ? extends K> keyMapper, SerFunction<? super T, ? extends V> valueMapper)
	{
		return t -> new SimpleEntry<>(keyMapper.apply(t), valueMapper.apply(t));
	}

	public static <K, V> SerFunction<K, Entry<K, V>> toEntryWithValue(SerFunction<? super K, ? extends V> keyToValueMapper)
	{
		return k -> new SimpleEntry<>(k, keyToValueMapper.apply(k));
	}

	public static <K, V> SerFunction<V, Entry<K, V>> toEntryWithKey(SerFunction<? super V, ? extends K> valueToKeyMapper)
	{
		return v -> new SimpleEntry<>(valueToKeyMapper.apply(v), v);
	}

	public static <U, V> SerCollector<Entry<? extends U, ? extends V>, ?, ConcurrentMap<U, V>> entriesToMap()
	{
		return SerCollector.from(toConcurrentMap(Entry::getKey, Entry::getValue));
	}

	public static <K, V> SerCollector<K, ?, ConcurrentMap<K, V>> toMapWithValues(SerFunction<K, V> keyMapper)
	{
		return SerCollector.from(toConcurrentMap(identity(), keyMapper));
	}

	public static <K, V> SerCollector<V, ?, ConcurrentMap<K, V>> toMapWithKeys(SerFunction<? super V, ? extends K> valueMapper)
	{
		return SerCollector.from(toConcurrentMap(valueMapper, identity()));
	}

	public static <U, V, M extends ConcurrentMap<U, V>> SerCollector<Entry<U, V>, ?, M> entriesToMap(SerSupplier<M> mapSupplier)
	{
		return SerCollector.from(toConcurrentMap(Entry::getKey, Entry::getValue, mergeError(), mapSupplier));
	}

	public static <U, V> SerCollector<Entry<U, V>, ?, ConcurrentMap<U, V>> entriesToMap(SerBinaryOperator<V> combiner)
	{
		return SerCollector.from(toConcurrentMap(Entry::getKey, Entry::getValue, combiner));
	}

	public static <U, V, R> SerCollector<Entry<U, V>, ?, ConcurrentMap<U, R>> mappingValues(SerFunction<? super V, ? extends R> valueMapper)
	{
		return SerCollector.from(toConcurrentMap(Entry::getKey, valueMapper.compose(Entry::getValue)));
	}

	public static <U, V, R> SerCollector<Entry<U, V>, ?, ConcurrentMap<R, V>> mappingKeys(SerFunction<? super U, ? extends R> keyMapper)
	{
		return SerCollector.from(toConcurrentMap(keyMapper.compose(Entry::getKey), Entry::getValue));
	}

	public static <U, V> SerCollector<Entry<U, V>, ?, ConcurrentMap<U, V>> mappingConcurrentEntries(SerSupplier<ConcurrentMap<U, V>> factory)
	{
		return SerCollector.from(toConcurrentMap(Entry::getKey, Entry::getValue, (a, b) -> {
			throw new UnsupportedOperationException("Duplicate");
		}, factory));
	}

	public static <U, V, R> SerCollector<Entry<U, V>, ?, ConcurrentMap<U, R>> groupingByKeys(SerCollector<Entry<U, V>, ?, R> valueMapper)
	{
		return SerCollector.from(groupingByConcurrent(Entry::getKey, valueMapper));
	}

	public static <T, R> R folding(R accumulator, Stream<T> stream, SerBiFunction<? super R, ? super T, ? extends R> aggregator)
	{
		for (T object : (Iterable<T>) () -> stream.iterator())
			accumulator = aggregator.apply(accumulator, object);
		return accumulator;
	}

	public static <T, R> R fold(R accumulator, Iterable<T> iterable, SerBiFunction<? super R, ? super T, ? extends R> aggregator)
	{
		for (T object : iterable)
			accumulator = aggregator.apply(accumulator, object);

		return accumulator;
	}

	public static <T> SerBinaryOperator<T> mergeError()
	{
		return (a, b) -> { 
			throw new UnsupportedOperationException("Found duplicated entries with same key: [" + a + ", " + b + "]");
		};
	}

	public static <T> SerBinaryOperator<T> mergeLeft()
	{
		return (a, b) -> a;
	}

	public static <T> SerBinaryOperator<T> mergeRight()
	{
		return (a, b) -> b;
	}

	public static <K, V> SerPredicate<Entry<K, V>> entryByValue(SerPredicate<V> valuePredicate)
	{
		return e -> valuePredicate.test(e.getValue());
	}

	public static <K, V> SerPredicate<Entry<K, V>> entryByKey(SerPredicate<K> keyPredicate)
	{
		return e -> keyPredicate.test(e.getKey());
	}

	public static <K, V> SerPredicate<? super Entry<K, V>> entryByKeyValue(SerBiPredicate<? super K, ? super V> entryPredicate)
	{
		return e -> entryPredicate.test(e.getKey(), e.getValue());
	}

	public static <K, V, R> SerFunction<Entry<K, V>, R> splitting(SerBiFunction<? super K, ? super V, R> biFunction)
	{
		return e -> biFunction.apply(e.getKey(), e.getValue());
	}

	public static <K, V, R> SerConsumer<Entry<K, V>> splittingConsumer(SerBiConsumer<? super K, ? super V> consumer)
	{
		return e -> consumer.accept(e.getKey(), e.getValue());
	}

	public static <A extends Serializable, B extends Serializable, C extends Serializable, R> SerFunction<Triple<A, B, C>, R> splitting(SerTriFunction<? super A, ? super B, ? super C, ? extends R> trifunction)
	{
		return t -> trifunction.apply(t.getFirst(), t.getSecond(), t.getThird());
	}

	public static <A extends Serializable, B extends Serializable, C extends Serializable> SerConsumer<Triple<A, B, C>> triple(TriConsumer<? super A, ? super B, ? super C> triconsumer)
	{
		return t -> triconsumer.accept(t.getFirst(), t.getSecond(), t.getThird());
	}

	public static <A extends Serializable, B extends Serializable, C extends Serializable, D extends Serializable> SerConsumer<Quadruple<A, B, C, D>> quartet(QuadConsumer<? super A, ? super B, ? super C, ? super D> quadconsumer)
	{
		return q -> quadconsumer.accept(q.getFirst(), q.getSecond(), q.getThird(), q.getFourth());
	}

	public static <A extends Serializable, B extends Serializable, C extends Serializable, D extends Serializable, R> SerFunction<Quadruple<A, B, C, D>, R> splitting(QuadFunction<? super A, ? super B, ? super C, ? super D, ? extends R> quadfunction)
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

	public static <T, U, A, R> SerCollector<T, A, R> flatMapping(SerFunction<? super T, ? extends Stream<? extends U>> mapper, SerCollector<? super U, A, R> downstream)
	{
		final SerBiConsumer<A, T> biConsumer = (r, t) -> 
			Optional.ofNullable(mapper.apply(t)).ifPresent(s -> s.forEach(u -> downstream.accumulator().accept(r, u)));

		return SerCollector.of(downstream.supplier()::get, biConsumer, downstream.combiner()::apply, downstream.finisher()::apply, downstream.characteristics());
	}

	public static <A1 extends Serializable, A2 extends Serializable, B extends Serializable, C extends Serializable> SerFunction<Triple<A1, B, C>, Triple<A2, B, C>> changingFirst(SerFunction<? super A1, ? extends A2> mapper)
	{
		return t -> new Triple<>(mapper.apply(t.getFirst()), t.getSecond(), t.getThird());
	}
	
	public static <T> SerUnaryOperator<Set<T>> retainer(Set<? extends T> toRetain)
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

	public static <A, B, C> SerFunction<A, SerFunction<B, C>> curry(SerBiFunction<A, B, C> biFunction)
	{
		return a -> b -> biFunction.apply(a, b);
	}

	public static <A, B> SerFunction<A, SerConsumer<B>> curry(SerBiConsumer<A, B> biconsumer)
	{
		return a -> b -> biconsumer.accept(a, b);
	}

	public static <A, B, C> SerFunction<B, SerFunction<A, C>> curryRev(SerBiFunction<A, B, C> biFunction)
	{
		return b -> a -> biFunction.apply(a, b);
	}
	
	public static <A, B> SerFunction<A, B> asFun(SerFunction<? super A, ? extends B> function)
	{
		return a -> function.apply(a);
	}
	
	public static <A> SerBinaryOperator<A> reverseIf(SerBinaryOperator<A> binaryOperator, boolean isReverse)
	{
		return (a, b) -> isReverse ? binaryOperator.apply(b, a) : binaryOperator.apply(a, b);
	}

	public static <A, R> SerBiFunction<? super A, ? super A, ? extends R> reverseIf(SerBiFunction<? super A, ? super A, ? extends R> biFunction, boolean isReverse)
	{
		return (a, b) -> isReverse ? biFunction.apply(b, a) : biFunction.apply(a, b);
	}

	public static SerBiPredicate<DataPoint, DataPoint> reverseIf(boolean isReverse, SerBiPredicate<DataPoint, DataPoint> predicate)
	{
		return (a, b) -> isReverse ? predicate.test(b, a) : predicate.test(a, b);
	}

	public static <A, B, C> SerBiFunction<A, B, C> asFun(SerBiFunction<? super A, ? super B, ? extends C> biFunction)
	{
		return (a, b) -> biFunction.apply(a, b);
	}
	
	@SuppressWarnings("ReturnValueIgnored")
	public <T> SerConsumer<T> asConsumer(SerFunction<? super T, ?> function)
	{
		return function::apply;
	}

	public static <T, U> SerBiPredicate<T, U> not(SerBiPredicate<T, U> test)
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

	public static <A extends Serializable, B extends Serializable, C extends Serializable, R extends Serializable> SerBiFunction<B, C, R> partial(SerTriFunction<A, B, C, R> triFunction, A a)
	{
		return (b, c) -> triFunction.apply(a, b, c);
	}

	public static <A, B, C, R> SerBiFunction<B, C, R> partial(SerTriFunction<A, B, C, R> triFunction, A a)
	{
		return (b, c) -> triFunction.apply(a, b, c);
	}
}
