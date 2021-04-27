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

import static java.lang.Long.MAX_VALUE;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Avoids StackOverflowError when concatenating long chains of multiple streams 
 * 
 * @author Valentino Pinna
 *
 * @param <T>
 */
public class ConcatSpliterator<T> implements Spliterator<T>
{
	private final Queue<Spliterator<T>> spliterators;
	
	public static <T> Collector<Stream<T>, ?, Stream<T>> concatenating(boolean keepOrder)
	{
		if (keepOrder)
			return collectingAndThen(toList(), collection -> StreamSupport.stream(new ConcatSpliterator<>(collection), !Utils.SEQUENTIAL));
		else
			return collectingAndThen(toSet(), collection -> StreamSupport.stream(new ConcatSpliterator<>(collection), !Utils.SEQUENTIAL));
	}
	
	public ConcatSpliterator(Collection<? extends Stream<T>> streams)
	{
		spliterators = Utils.SEQUENTIAL ? new LinkedList<>() : new ConcurrentLinkedQueue<>(); 
		for (Stream<T> stream: streams)
			spliterators.add(stream.spliterator());
	}
	
	public ConcatSpliterator(Queue<Spliterator<T>> spliterators)
	{
		this.spliterators = spliterators;
	}
	
	@Override
	public Spliterator<T> trySplit()
	{
		return spliterators.poll();
	}

	@Override
	public boolean tryAdvance(Consumer<? super T> consumer)
	{
		while (!spliterators.isEmpty())
			if (spliterators.peek().tryAdvance(consumer))
				return true;
			else
				spliterators.remove();
		return false;
	}

	@Override
	public void forEachRemaining(Consumer<? super T> consumer)
	{
		Spliterator<T> spliterator;
		while ((spliterator = spliterators.poll()) != null)
			spliterator.forEachRemaining(consumer);
	}

	@Override
	public long estimateSize()
	{
		long size = 0;
		for (Spliterator<T> spliterator: spliterators)
		{
			size += spliterator.estimateSize();
			if (size < 0)
				return MAX_VALUE;
		}
		
		return size;
	}

	@Override
	public int characteristics()
	{
		// The initial value 0 is not actually used
		int characteristics = 0; 
		boolean first = true;
		for (Spliterator<T> spliterator: spliterators)
		{
			if (first)
			{
				first = false;
				characteristics = spliterator.characteristics();
			}
			else
				// distinct and sorted lost if more than 1 spliterator in queue
				characteristics &= spliterator.characteristics() & ~(DISTINCT | SORTED);
		}
		
		return characteristics;
	}
}
