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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
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
	public static final int EPSILON = Integer.parseInt(System.getProperty("vtl.double.epsilon.digits", "12"));
	
	private Utils() {}

	public static <K, V1, V2> SerFunction<Entry<K, V1>, Entry<K, V2>> keepingKey(SerFunction<? super V1, ? extends V2> valueMapper)
	{
		return e -> new SimpleEntry<>(e.getKey(), valueMapper.apply(e.getValue()));
	}

	public static <R> SerUnaryOperator<R> applyIf(SerPredicate<? super R> condition, SerUnaryOperator<R> mapper)
	{
		return v -> condition.test(v) ? mapper.apply(v) : v;
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

	public static <K, V, R> SerFunction<Entry<K, V>, R> splitting(SerBiFunction<K, V, R> biFunction)
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
	
	public static <T> T coalesce(T value, T defaultValue)
	{
		return value != null ? value : defaultValue;
	}

	public static <T> T coalesce(T value, SerSupplier<? extends T> defaultValue)
	{
		return value != null ? value : defaultValue.get();
	}

	public static <T, R> R tryWith(SerSupplier<? extends Stream<T>> supplier, SerFunction<? super Stream<T>, R> consumer)
	{
		try (Stream<T> stream = supplier.get())
		{
			return consumer.apply(stream);
		}
	}
}
