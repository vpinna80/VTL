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
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
	public static final int ULPS = Integer.parseInt(System.getProperty("vtl.double.ulps.epsilon", "1000"));
	
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

	public static <R> SerUnaryOperator<R> applyIf(SerPredicate<? super R> condition, SerUnaryOperator<R> mapper)
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
	
	public static <T> void tryWith(SerSupplier<? extends Stream<T>> supplier, SerConsumer<? super Stream<T>> consumer)
	{
		try (Stream<T> stream = supplier.get())
		{
			consumer.accept(stream);
		}
	}

	public static <T, R> R tryWith(SerSupplier<? extends Stream<T>> supplier, SerFunction<? super Stream<T>, R> consumer)
	{
		try (Stream<T> stream = supplier.get())
		{
			return consumer.apply(stream);
		}
	}
}
