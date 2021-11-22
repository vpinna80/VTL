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

import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.getEncoderForComponent;
import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.scalarFromColumnValue;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DayHolder;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.OptionalBox;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class VTLSparkAggregator<A> extends Aggregator<Serializable, A, Serializable>
{
	private static final long serialVersionUID = 1L;

	private final Encoder<A> accEncoder;
	private final Encoder<?> resultEncoder;
	private final SerCollector<ScalarValue<?, ?, ?, ?>, A, A> coll;
	private final DataStructureComponent<?, ?, ?> oldComp;

	@SuppressWarnings("unchecked")
	public VTLSparkAggregator(DataStructureComponent<?, ?, ?> oldComp, DataStructureComponent<?, ?, ?> newComp,
			SerCollector<ScalarValue<?, ?, ?, ?>, ?, A> collector, SparkSession session)
	{
		try
		{
			this.coll = (SerCollector<ScalarValue<?, ?, ?, ?>, A, A>) collector;
			this.oldComp = oldComp;
			
			A zero = zero();
			if (zero instanceof double[])
			{
				accEncoder = (Encoder<A>) session.implicits().newDoubleArrayEncoder();
				resultEncoder = getEncoderForComponent(oldComp);
			}
			else if (zero instanceof OptionalBox)
			{
				accEncoder = (Encoder<A>) Encoders.kryo(OptionalBox.class);
				resultEncoder = getEncoderForComponent(oldComp);
			}
			else if (zero instanceof ArrayList)
			{
				accEncoder = (Encoder<A>) Encoders.kryo(ArrayList.class);
				resultEncoder = Encoders.kryo(Serializable[].class);
			}
			else
				throw new UnsupportedOperationException("Spark encoder not found for " + zero.getClass().getName());
		}
		catch (RuntimeException e) 
		{
			throw e;
		}
	}
	
	@Override
	public A zero()
	{
		return coll.supplier().get();
	}

	@Override
	public Encoder<A> bufferEncoder()
	{
		return (Encoder<A>) accEncoder;
	}

	@Override
	public A reduce(A acc, Serializable value)
	{
		coll.accumulator().accept(acc, scalarFromColumnValue(value, oldComp));
		return acc;
	}

	@Override
	public A merge(A acc1, A acc2)
	{
		return coll.combiner().apply(acc1, acc2);
	}

	@Override
	public Serializable finish(A reduction)
	{
		final A result = coll.finisher().apply(reduction);
		if (result instanceof ArrayList)
			return ((ArrayList<?>) result).stream()
				.map(ScalarValue.class::cast)
				.map(ScalarValue::get)
				.map(v -> v instanceof DayHolder ? ((DayHolder) v).getLocalDate() : v)
				.collect(toList())
				.toArray(new Serializable[0]);
		else if (result instanceof DateValue)
			return ((DayHolder) ((DateValue<?>) result).get()).getLocalDate();
		else if (result instanceof ScalarValue)
			return ((ScalarValue<?, ?, ?, ?>) result).get();
		else
			throw new UnsupportedOperationException("Class not implemented as finished value in spark aggregator: " + result.getClass().getName());
	}

	@SuppressWarnings("unchecked")
	@Override
	public Encoder<Serializable> outputEncoder()
	{
		return (Encoder<Serializable>) resultEncoder;
	}
	
	@Override
	public String toString()
	{
		return "VTLSparkAggregator(" + oldComp + ")";
	}
};
