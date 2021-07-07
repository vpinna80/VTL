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

import java.io.Serializable;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;

import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.OptionalBox;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class AnalyticAggregator<A> extends Aggregator<Serializable, A, Serializable>
{
	private static final long serialVersionUID = 1L;

	private final Encoder<A> accEncoder;
	private final Encoder<?> resultEncoder;
	private final SerCollector<ScalarValue<?, ?, ?, ?>, A, A> coll;
	private final DataStructureComponent<?, ?, ?> oldMeasure;

	@SuppressWarnings("unchecked")
	public AnalyticAggregator(DataStructureComponent<?, ?, ?> oldMeasure, DataStructureComponent<?, ?, ?> newMeasure,
			SerCollector<ScalarValue<?, ?, ?, ?>, ?, A> collector, SparkSession session)
	{
		this.resultEncoder = getEncoderForComponent(newMeasure);
		
		A zero = (A) collector.supplier().get();
		if (zero instanceof double[])
			accEncoder = (Encoder<A>) session.implicits().newDoubleArrayEncoder();
		else if (zero instanceof OptionalBox)
			accEncoder = (Encoder<A>) Encoders.kryo(OptionalBox.class);
		else
			throw new UnsupportedOperationException("Spark encoder not found for " + zero.getClass().getName());

		this.coll = (SerCollector<ScalarValue<?, ?, ?, ?>, A, A>) collector;
		this.oldMeasure = oldMeasure;
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
		coll.accumulator().accept(acc, scalarFromColumnValue(value, oldMeasure));
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
		return ((ScalarValue<?, ?, ?, ?>) coll.finisher().apply(reduction)).get();
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
		return "VTLAggregator(" + oldMeasure + ")";
	}
};
