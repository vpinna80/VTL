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

import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.PRIM_BUILDERS;

import java.io.Serializable;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class VTLSparkAggregator<I, TT> extends Aggregator<I, Object, TT>
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLSparkAggregator.class);

	private final Encoder<Object> accEncoder;
	private final Encoder<TT> resultEncoder;
	private final SerCollector<I, Object, TT> collector;

	@SuppressWarnings("unchecked")
	public VTLSparkAggregator(SerCollector<I, ?, TT> collector, Encoder<?> accEncoder, Encoder<TT> resultEncoder)
	{
		this.collector =  (SerCollector<I, Object, TT>) collector;
		this.resultEncoder = resultEncoder;
		this.accEncoder = (Encoder<Object>) accEncoder;
	}
	
	@Override
	public Object zero()
	{
		return collector.supplier().get();
	}

	@Override
	public Encoder<Object> bufferEncoder()
	{
		return accEncoder;
	}

	@Override
	public Object reduce(Object acc, I value)
	{
		// For performance reasons scalars are encoded as boxed primitive types, and must be rebuilt
		if (value != null && PRIM_BUILDERS.containsKey(value.getClass()))
			value = (I) PRIM_BUILDERS.get(value.getClass()).apply((Serializable) value);
		
		collector.accumulator().accept(acc, value);
		return acc;
	}

	@Override
	public Object merge(Object acc1, Object acc2)
	{
		return collector.combiner().apply(acc1, acc2);
	}

	@Override
	public TT finish(Object reduction)
	{
		Object apply = collector.finisher().apply(reduction);
		if (apply instanceof ScalarValue)
			apply = ((ScalarValue<?, ?, ?, ?>) apply).get();
		
		LOGGER.debug("Finished Spark aggregation: {} of {}", apply, apply == null ? null : apply.getClass());
		
		@SuppressWarnings("unchecked")
		TT result = (TT) apply;
		return result;
	}

	@Override
	public Encoder<TT> outputEncoder()
	{
		return resultEncoder;
	}
}
