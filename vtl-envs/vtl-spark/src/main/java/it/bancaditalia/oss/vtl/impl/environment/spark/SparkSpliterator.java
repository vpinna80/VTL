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

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.apache.spark.sql.Row;

final class SparkSpliterator implements Spliterator<Row>
{
	private final int batchSize;
	private final Iterator<Row> iterator;
	private final ArrayDeque<Row> batch;
	
	public SparkSpliterator(Iterator<Row> iterator, int batchSize)
	{
		this.iterator = iterator;
		this.batchSize = batchSize;
		this.batch = new ArrayDeque<>(batchSize);
		
		updateBatch();
	}

	@Override
	public boolean tryAdvance(Consumer<? super Row> action)
	{
		Row row = batch.poll();
		
		if (row != null)
		{
			action.accept(row);
			return true;
		}
		
		updateBatch();
		return batch.isEmpty() ? false : tryAdvance(action);
	}

	private void updateBatch()
	{
		synchronized (iterator)
		{
			for (int i = 0; i < batchSize && iterator.hasNext(); i++)
				batch.add(iterator.next());
		}
	}

	@Override
	public Spliterator<Row> trySplit()
	{
		boolean canSplit;
		synchronized (iterator)
		{
			canSplit = iterator.hasNext();
		}
		
		return canSplit ? new SparkSpliterator(iterator, batchSize) : null;
	}

	@Override
	public long estimateSize()
	{
		return Long.MAX_VALUE;
	}

	@Override
	public int characteristics()
	{
		return CONCURRENT | NONNULL;
	}
}