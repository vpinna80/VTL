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

import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1D;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1M;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1Q;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1S;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1W;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1Y;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;

import java.io.Serializable;
import java.time.Period;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.function.Supplier;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.date.Halves;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.Quarters;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.util.SerFunction;

public enum Frequency implements Serializable, Supplier<DurationValue>, Comparable<Frequency>
{
	// DO NOT CHANGE THE ORDERING!!
	// Must be sorted from largest to smallest
	A(P1Y, YearPeriodHolder::new),
	S(P1S, SemesterPeriodHolder::new), 
	Q(P1Q, QuarterPeriodHolder::new), 
	M(P1M, MonthPeriodHolder::new), 
	W(P1W, WeekPeriodHolder::new), 
	D(P1D, null);
	
	private final DurationValue value;
	private final TemporalAmount period;
	private final SerFunction<TemporalAccessor, ? extends PeriodHolder<?>> holderAllocator;
	
	private Frequency(TemporalAmount period, SerFunction<TemporalAccessor, ? extends PeriodHolder<?>> holderAllocator)
	{
		this.period = period;
		this.holderAllocator = holderAllocator;
		this.value = new DurationValue(this);
	}

	public TimePeriodValue<?> wrap(TimeValue<?, ?, ?, ?> toWrap)
	{
		if (toWrap instanceof TimePeriodValue || toWrap instanceof DateValue)
		{
			TemporalAccessor accessor = ((TemporalAccessor) toWrap.get());
			return TimePeriodValue.of(holderAllocator.apply(accessor));
		}
		else
			throw new VTLCastException(TIMEDS, toWrap);
	}
	
	@Override
	public DurationValue get()
	{
		return value;
	}
	
	public TemporalAmount getPeriod()
	{
		return period;
	}
	
	public TemporalAmount getScaledPeriod(int factor)
	{
		if (period instanceof Period)
			return ((Period) period).multipliedBy(factor);
		else if (P1S.equals(period))
			return Halves.of(factor);
		else if (P1Q.equals(period))
			return Quarters.of(factor);
		else
			throw new UnsupportedOperationException("getScaledPeriod for " + period);
	}
	
	public boolean isMultiple(TemporalAmount other)
	{
		if (period == DurationValue.P1D)
			return true;
		else if (period == DurationValue.P1W)
			return false;

		return other.get(DAYS) == 0 && (other.get(YEARS) * 12 + other.get(MONTHS)) % (period.get(YEARS) * 12 + period.get(MONTHS)) == 0; 
	}
	
	public final int compareWith(Frequency o)
	{
		return Integer.compare(o.ordinal(), ordinal());
	}
}