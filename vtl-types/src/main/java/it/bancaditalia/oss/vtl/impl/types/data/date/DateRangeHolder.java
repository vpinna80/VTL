package it.bancaditalia.oss.vtl.impl.types.data.date;

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

public class DateRangeHolder implements Serializable, Comparable<DateRangeHolder>, TemporalAccessor
{
	private static final long serialVersionUID = 1L;
	
	private final TimeValue<?, ?, ?, ?> start;
	private final TimeValue<?, ?, ?, ?> end;
	private final TemporalAmount length;
	private final int hash;
	
	private transient DurationValue frequency;

	public DateRangeHolder(TimeValue<?, ?, ?, ?> start, TimeValue<?, ?, ?, ?> end)
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
	public int compareTo(DateRangeHolder o)
	{
		int c = start.compareTo(o.start);
		return c != 0 ? c : end.compareTo(o.end);
	}

	public DateRangeHolder incrementSmallest(long amount)
	{
		TimeValue<?, ?, ?, ?> newStart = end.add(1);
		TimeValue<?, ?, ?, ?> newEnd = newStart.add(length).add(-1);
		
		return new DateRangeHolder(newStart, newEnd);
	}

	public DateRangeHolder increment(TemporalAmount period)
	{
		return new DateRangeHolder(start.add(period), end.add(period));
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
		
		DateRangeHolder o = (DateRangeHolder) obj;
		
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
		return start.until(((DateRangeHolder) end.get()).start);
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
}
