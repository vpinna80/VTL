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
package it.bancaditalia.oss.vtl.impl.types.data.date;

import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1D;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1M;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1Q;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1S;
import static it.bancaditalia.oss.vtl.impl.types.data.DurationValue.P1Y;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.time.LocalDate;
import java.time.Period;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.util.List;
import java.util.Objects;

import org.threeten.extra.LocalDateRange;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;

public class TimeRangeHolder implements Serializable, Comparable<TimeRangeHolder>, TemporalAccessor
{
	private static final long serialVersionUID = 1L;
	
	private final TimeValue<?, ?, ?, ?> start;
	private final TimeValue<?, ?, ?, ?> end;
	private final TemporalAmount length;
	private final int hash;
	
	private transient DurationValue frequency;

	public TimeRangeHolder(TimeValue<?, ?, ?, ?> start, TimeValue<?, ?, ?, ?> end)
	{
		if (start instanceof GenericTimeValue || end instanceof GenericTimeValue)
			throw new InvalidParameterException("GenericTimeValue cannot be nested.");
		
		this.start = requireNonNull(start, "Unbounded times are not supported");
		this.end = requireNonNull(end, "Unbounded times are not supported");
		
		LocalDate dStart = start instanceof DateValue ? (LocalDate) start.get() : ((PeriodHolder<?>) start.get()).startDate();
		LocalDate dEnd = (end instanceof DateValue ? (LocalDate) end.get() : ((PeriodHolder<?>) end.get()).endDate()).plusDays(1);

		this.hash = Objects.hash(dStart, dEnd);

		Period length = Period.between(dStart, dEnd);
		long months = length.toTotalMonths();
		
		if (length.getDays() == 0)
			for (TemporalAmount p: List.of(P1Y, P1S, P1Q, P1M))
				if (p.get(YEARS) * 12 + p.get(MONTHS) == months)
				{
					this.length = p;
					return;
				}
		
		if (length.getDays() == 1 && length.getMonths() == 0 && length.getYears() == 0)
		{
			this.length = P1D;
			return;
		}

		throw new UnsupportedOperationException("Unsupported time value length: " + length);
	}

	@Override
	public boolean isSupported(TemporalField field)
	{
		return false;
	}

	@Override
	public long getLong(TemporalField field)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int compareTo(TimeRangeHolder o)
	{
		int c = start.compareTo(o.start);
		return c != 0 ? c : end.compareTo(o.end);
	}

	public TimeRangeHolder incrementSmallest(long amount)
	{
		long i = amount;
		TimeValue<?, ?, ?, ?> newStart = start;
		TimeValue<?, ?, ?, ?> newEnd = end;
		for (; i > 0; i--)
		{
			newStart = newStart.add(length);
			newEnd = newEnd.add(length);
		}
		for (; i < 0; i++)
		{
			newStart = newStart.minus(length);
			newEnd = newEnd.minus(length);
		}

		return new TimeRangeHolder(newStart, newEnd);
	}

	public TimeRangeHolder increment(TemporalAmount period)
	{
		return new TimeRangeHolder(start.add(period), end.add(period));
	}
	
	public TimeRangeHolder decrement(TemporalAmount period)
	{
		return new TimeRangeHolder(start.minus(period), end.minus(period));
	}
	
	@Override
	public String toString()
	{
		return start + "/" + end;
	}

	@Override
	public int hashCode()
	{
		return hash;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		TimeRangeHolder o = (TimeRangeHolder) obj;
		
		if (length.equals(o.length))
		{
			LocalDate dStart = start instanceof DateValue ? (LocalDate) start.get() : ((PeriodHolder<?>) start.get()).startDate();
			LocalDate doStart = o.start instanceof DateValue ? (LocalDate) o.start.get() : ((PeriodHolder<?>) o.start.get()).startDate();
		
			return dStart.equals(doStart);
		}
		
		return false;
	}

	public Period until(TimeValue<?, ?, ?, ?> end)
	{
		return start.until(((TimeRangeHolder) end.get()).start);
	}

	public DurationValue getFrequency()
	{
		if (frequency != null)
			return frequency;
		
		LocalDate dStart = start instanceof DateValue ? (LocalDate) start.get() : ((PeriodHolder<?>) start.get()).startDate();
		LocalDate dEnd = end instanceof DateValue ? (LocalDate) end.get() : ((PeriodHolder<?>) end.get()).endDate();
		Period period = LocalDateRange.ofClosed(dStart, dEnd).toPeriod();
		
		for (Frequency freq: Frequency.values())
			if (freq.getPeriod().equals(period))
				return frequency = freq.get();
		
		if (Period.ofMonths(6).equals(period))
			return Frequency.S.get();
		else if (Period.ofMonths(3).equals(period))
			return Frequency.Q.get();
		
		throw new UnsupportedOperationException("Unsupported frequency " + period + " for " + this);
	}

	public DateValue<?> getStartDate()
	{
		return start.getStartDate();
	}

	public DateValue<?> getEndDate()
	{
		return end.getEndDate();
	}

	public TimeValue<?, ?, ?, ?> getStartTime()
	{
		return start;
	}

	public TimeValue<?, ?, ?, ?> getEndTime()
	{
		return end;
	}
}
