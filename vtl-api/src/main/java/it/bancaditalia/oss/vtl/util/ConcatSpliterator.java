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

import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static java.lang.Long.MAX_VALUE;

import java.util.Collection;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Avoids StackOverflowError when concatenating long chains of multiple streams 
 * 
 * @author Valentino Pinna
 *
 * @param <T>
 */
public class ConcatSpliterator<T> implements Spliterator<T>
{
	private final Spliterator<T>[] array;
	private volatile int current = 0;
	private volatile int index = 0;
	private transient long estimateSize = 0;
	
	/**
	 * Constructs a collector that concatenates a {@code Stream<Stream<T>>}, similar to {@link Stream#flatMap}.
	 * It maintains the parallelism of the stream, and optionally the encounter order of elements.
	 * It avoids the overhead of executing many {@link Stream#concat} operations.
	 * 
	 * @param <T> The type of elements.
	 * @param keepOrder True if the collector should maintain the encounter order.
	 * @return The collector.
	 */
	public static <T> SerCollector<Stream<T>, ?, Stream<T>> concatenating(boolean keepOrder)
	{
		return collectingAndThen(toList(), streams -> {
			Stream<T> result = null;
			for (Stream<T> stream: streams)
				if (result == null)
					result = stream;
				else
					result = Stream.concat(result, stream);
			
			return result;
		});
	}
	
	@SuppressWarnings("unchecked")
	private ConcatSpliterator(Collection<Spliterator<T>> spliterators)
	{
		this.array = (Spliterator<T>[]) spliterators.toArray(new Spliterator<?>[spliterators.size()]);
		
		for (int i = array.length - 1; i >= 0; i--)
		{
			long estimatedSize = array[i].estimateSize();
			if (estimatedSize == MAX_VALUE)
				estimateSize = MAX_VALUE;
			else if (estimateSize < MAX_VALUE)
			{
				estimateSize += estimatedSize;
				if (estimateSize < 0)
					estimateSize = MAX_VALUE;
			}
		}
	}

	@Override
	public Spliterator<T> trySplit()
	{
		index = current++;
		if (index >= array.length)
			return null;
		
		if (estimateSize != MAX_VALUE)
			estimateSize -= array[index].estimateSize();
		
		final Spliterator<T> spliterator = array[index];
		array[index] = null;
		return spliterator;
	}

	@Override
	public boolean tryAdvance(Consumer<? super T> consumer)
	{
		while (index < array.length)
		{
			Spliterator<T> spliterator = array[index];
			if (spliterator != null && spliterator.tryAdvance(consumer))
			{
				if (estimateSize != MAX_VALUE)
					estimateSize--;
				return true;
			}
			else
			{
				array[index] = null;
				index = current++;
			}
		}
		
		return false;
	}

	@Override
	public void forEachRemaining(Consumer<? super T> consumer)
	{
		while (index < array.length)
		{
			final Spliterator<T> spliterator = array[index];
			array[index] = null;
			index = current++;
			
			if (spliterator != null)
				spliterator.forEachRemaining(consumer);
		}
		
		estimateSize = 0;
	}

	@Override
	public long estimateSize()
	{
		return estimateSize;
	}

	@Override
	public int characteristics()
	{
		// Report all characteristics if the spliterator is completely empty
		if (index >= array.length)
			return IMMUTABLE | NONNULL | SIZED | SUBSIZED | ORDERED;
		
		int characteristics = IMMUTABLE | NONNULL | SIZED | SUBSIZED | ORDERED; 
		for (int i = index; i < array.length; i++)
			if (array[i] != null)
				characteristics &= array[i].characteristics();
		
		// distinct and sorted lost when combining spliterators
		return characteristics & ~(DISTINCT | SORTED);
	}
}
