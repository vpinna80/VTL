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
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.function.Supplier;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
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