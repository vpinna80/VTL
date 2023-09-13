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

import static org.apache.spark.sql.functions.col;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

final class SparkSpliterator implements Spliterator<Row>
{
	private final long numPartitions;
	private final int batchSize;
	private final Dataset<Row> dataframe;
	private final AtomicInteger reservedPartitionNumber;
	private int currentPartitionNumber;
	private Iterator<Row> currentPartition;

	public SparkSpliterator(Dataset<Row> dataframe, int batchSize, long numPartitions)
	{
		this.numPartitions = numPartitions;
		this.batchSize = batchSize;
		this.dataframe = dataframe;
		this.reservedPartitionNumber = new AtomicInteger(0);
		this.currentPartitionNumber = 0;
		this.currentPartition = dataframe.where(col("$id$").eqNullSafe(0)).drop("$id$").toLocalIterator();
	}

	public SparkSpliterator(Dataset<Row> partitioned, int batchSize, long numPartitions, AtomicInteger reservedPartitionNumber)
	{
		this.numPartitions = numPartitions;
		this.batchSize = batchSize;
		this.dataframe = partitioned;
		this.reservedPartitionNumber = reservedPartitionNumber;
		updatePartition();
	}

	@Override
	public long estimateSize()
	{
		return (numPartitions + 1 - reservedPartitionNumber.get()) * batchSize;
	}

	@Override
	public int characteristics()
	{
		return CONCURRENT | NONNULL;
	}

	@Override
	public synchronized boolean tryAdvance(Consumer<? super Row> action)
	{
		if (currentPartition != null && currentPartition.hasNext())
		{
			action.accept(currentPartition.next());
			return true;
		}
		
		updatePartition();
		return currentPartition != null ? tryAdvance(action) : false;
	}

	private void updatePartition()
	{
		if (currentPartition != null)
		{
			currentPartitionNumber = reservedPartitionNumber.getAndIncrement();
			if (currentPartitionNumber < numPartitions)
				currentPartition = dataframe.where(col("$id$").eqNullSafe(currentPartitionNumber)).toLocalIterator();
			else
				currentPartition = null;
		}
	}

	@Override
	public Spliterator<Row> trySplit()
	{
		return currentPartitionNumber < numPartitions ? new SparkSpliterator(dataframe, batchSize, numPartitions, reservedPartitionNumber) : null;
	}
}