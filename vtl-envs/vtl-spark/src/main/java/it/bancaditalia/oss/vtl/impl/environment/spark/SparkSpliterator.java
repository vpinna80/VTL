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
package it.bancaditalia.oss.vtl.impl.environment.spark;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import org.apache.spark.sql.Row;

final class SparkSpliterator implements Spliterator<Row>
{
	private final BlockingQueue<Row> queue;
	private final Thread collector;
	private final List<Row> section = new ArrayList<>();
	
	private volatile int current = 0;

	SparkSpliterator(BlockingQueue<Row> queue, Thread collector)
	{
		this.queue = queue;
		this.collector = collector;
	}

	@Override
	public boolean tryAdvance(Consumer<? super Row> action)
	{
		while (current >= section.size())
			try
			{
				updateSection();

				if (current >= section.size() && !collector.isAlive())
					return false;
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
				Thread.currentThread().interrupt();
				return false;
			}
		
		action.accept(requireNonNull(section.get(current++)));
		return true;
	}

	private synchronized void updateSection() throws InterruptedException
	{
		section.clear();
		queue.drainTo(section);
		current = 0;
		if (section.size() == 0)
			wait(500);
	}

	@Override
	public Spliterator<Row> trySplit()
	{
		return collector.isAlive() ? new SparkSpliterator(queue, collector) : null;
	}

	@Override
	public long estimateSize()
	{
		return -1;
	}

	@Override
	public int characteristics()
	{
		return CONCURRENT | NONNULL;
	}
}