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
package it.bancaditalia.oss.vtl.impl.types.data;

import static it.bancaditalia.oss.vtl.impl.types.data.Frequency.A;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.time.Period;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.util.SerSupplier;

public abstract class TimeValue<I extends TimeValue<I, R, S, D>, R extends Comparable<? super R> & TemporalAccessor & Serializable, S extends TimeDomainSubset<S, D>, D extends TimeDomain> 
		extends BaseScalarValue<I, R, S, D>
{
	private static final long serialVersionUID = 1L;

	public static class FillTimeSeriesHolder implements Serializable
	{
		private static final long serialVersionUID = 1L;
		
		private DurationValue duration = A.get();
		private TimeValue<?, ?, ?, ?>[] key = new TimeValue<?, ?, ?, ?>[6];
		private int index = 3;
		
		public TimeValue<?, ?, ?, ?> get(int i)
		{
			return key[i];
		}

		public void add(TimeValue<?, ?, ?, ?> value)
		{
			key[index++] = value;
		}

		public DurationValue getDuration()
		{
			return duration;
		}

		public void setDuration(DurationValue duration)
		{
			this.duration = duration;
		}

		public void setConstants(DurationValue duration, SerSupplier<ScalarValue<?, ?, ?, ?>> min, SerSupplier<ScalarValue<?, ?, ?, ?>> max, SerSupplier<ScalarValue<?, ?, ?, ?>> end)
		{
			this.duration = duration;
			if (key[0] == null)
			{
				ScalarValue<?, ?, ?, ?> minV = min.get();
				key[0] = minV.isNull() ? null : (TimeValue<?, ?, ?, ?>) minV;
				ScalarValue<?, ?, ?, ?> maxV = max.get();
				key[1] = maxV.isNull() ? null : (TimeValue<?, ?, ?, ?>) maxV;
				ScalarValue<?, ?, ?, ?> endV = end.get();
				key[2] = endV.isNull() ? null : (TimeValue<?, ?, ?, ?>) endV;
			}
			
		}
	}

	public static class FillTimeSeriesTimeList implements Serializable
	{
		private static final long serialVersionUID = 1L;

		private TimeValue<?, ?, ?, ?>[] list;
		
		public FillTimeSeriesTimeList()
		{
			list = null;
		}
		
		public TimeValue<?, ?, ?, ?>[] list()
		{
			return list;
		}
		
		public FillTimeSeriesTimeList list(TimeValue<?, ?, ?, ?>[] list)
		{
			this.list = list;
			return this;
		}
		
		public List<TimeValue<?, ?, ?, ?>> getList()
		{
			return Arrays.asList(list);
		}

		public FillTimeSeriesTimeList setList(List<TimeValue<?, ?, ?, ?>> list)
		{
			this.list = list.toArray(TimeValue<?, ?, ?, ?>[]::new);
			return this;
		}
	}

	public TimeValue(R value, S domain)
	{
		super(value, domain);
	}

	/**
	 * @return The first date included in this TimeValue
	 */
	public abstract DateValue<?> getStartDate();
	
	/**
	 * 
	 * @return The last date included in this TimeValue
	 */
	public abstract DateValue<?> getEndDate();
	
	/**
	 * Determines the intrinsic frequency of this time index expressed as a {@link DurationValue}
	 * 
	 * @return
	 */
	public abstract DurationValue getFrequency();

	/**
	 * Create a new TimeValue by adding a given number of the smallest periods to this TimeValue.
	 * 
	 * @param periods The numbers of periods to increment 
	 * @return a new incremented TimeValue
	 */
	public abstract I add(long periods);

	/**
	 * Create a new TimeValue by adding a given period to this TimeValue.
	 * If the period isn't aligned with this TimeValue, an exception is thrown. 
	 * 
	 * @param length The {@link TemporalAmount} to increment this TimeVale of
	 * @return a new incremented TimeValue
	 * @throws InvalidParameterException if the period is not aligned with this TimeValue intrinsic period.
	 */
	public abstract I add(TemporalAmount length);

	/**
	 * Create a new TimeValue by subtracting a given period to this TimeValue.
	 * If the period isn't aligned with this TimeValue, an exception is thrown. 
	 * 
	 * @param length The TemporalAmount to decrement this TimeVale of
	 * @return a new decremented TimeValue
	 * @throws InvalidParameterException if the period is not aligned with this TimeValue intrinsic period.
	 */
	public abstract TimeValue<?, ?, ?, ?> minus(TemporalAmount length);

	/**
	 * Determines the period lasting from the beginning of this TimeValue until the beginning of given TimeValue (excluded).
	 * 
	 * @param end the reference TimeValue
	 * @return a {@link Period}
	 */
	public abstract Period until(TimeValue<?, ?, ?, ?> end);
}
