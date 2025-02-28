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

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class VTLSparkAggregator<I, A, TT, S extends ScalarValue<?, ?, ?, ?>> extends Aggregator<I, A, TT>
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLSparkAggregator.class);

	private final Encoder<A> accEncoder;
	private final Encoder<TT> resultEncoder;
	private final SerCollector<I, A, TT> collector;

	@SuppressWarnings("unchecked")
	public VTLSparkAggregator(SerCollector<I, ?, TT> collector, Encoder<A> accEncoder, Encoder<TT> resultEncoder)
	{
		this.collector =  (SerCollector<I, A, TT>) collector;
		this.accEncoder = accEncoder;
		this.resultEncoder = resultEncoder;
	}

	@Override
	public A reduce(A acc, I value)
	{
		collector.accumulator().accept(acc, value);
		return acc instanceof NullValue ? null : acc;
	}

	@Override
	public A merge(A acc1, A acc2)
	{
		A acc = collector.combiner().apply(acc1, acc2);
		if (acc == null)
			throw new IllegalStateException();

		return acc instanceof NullValue ? null : acc;
	}

	@Override
	public TT finish(A reduction)
	{
		TT result = collector.finisher().apply(reduction);
		LOGGER.debug("Finished Spark aggregation: {} of {}", result, result == null ? null : result.getClass());

		return result.getClass() != NullValue.class ? result : null;
	}
	
	@Override
	public A zero()
	{
		return collector.supplier().get();
	}

	@Override
	public Encoder<A> bufferEncoder()
	{
		return accEncoder;
	}

	@Override
	public Encoder<TT> outputEncoder()
	{
		return resultEncoder;
	}
}
