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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.isUseBigDecimal;
import static java.math.BigDecimal.ZERO;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class SerDoubleSumAvgCount implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final AtomicInteger count = new AtomicInteger(0);
	private final AtomicInteger countDouble = new AtomicInteger(0);
	private final DoubleAdder doubleSum = new DoubleAdder();
	private final LongAdder longSum = new LongAdder(); 
	private final AtomicReference<BigDecimal> bigDecimalSum = new AtomicReference<>(ZERO);

    public void accumulate(Double d)
    {
		count.incrementAndGet();
		countDouble.incrementAndGet();
		doubleSum.add(d);
    }

    public void accumulate(Long l)
    {
		count.incrementAndGet();
		longSum.add(l);
    }

    public void accumulate(ScalarValue<?, ?, ?, ?> scalar)
    {
    	if (scalar == null)
    		return;
		Number value = (Number) scalar.get();
		if (value == null)
			return;
		
		count.incrementAndGet();
		if (value.getClass() == Long.class)
			longSum.add(value.longValue());
		else
		{
			countDouble.incrementAndGet();
			if (isUseBigDecimal())
				bigDecimalSum.accumulateAndGet((BigDecimal) value, BigDecimal::add);
			else
				doubleSum.add(value.doubleValue());
		}
    }

    public SerDoubleSumAvgCount combine(SerDoubleSumAvgCount other)
    {
    	throw new UnsupportedOperationException("SerDoubleSumAvgCount::combine");
    }

	public int getCount()
	{
		return count.get();
	}

	public int getCountDouble()
	{
		return countDouble.get();
	}

	public Long getLongSum()
	{
		return longSum.sum();
	}

	public BigDecimal getBigDecimalSum()
	{
		return bigDecimalSum.get();
	}

	public double getDoubleSum()
	{
		return doubleSum.sum();
	}
}
