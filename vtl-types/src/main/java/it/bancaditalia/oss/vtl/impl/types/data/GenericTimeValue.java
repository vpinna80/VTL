package it.bancaditalia.oss.vtl.impl.types.data;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import java.time.Period;
import java.time.temporal.TemporalAmount;

import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimeDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;

public class GenericTimeValue<S extends TimeDomainSubset<S, TimeDomain>> extends TimeValue<GenericTimeValue<S>, TimeRangeHolder, S, TimeDomain>
{
	private GenericTimeValue(TimeRangeHolder value, S domain)
	{
		super(value, domain);
	}

	public static ScalarValue<?, ?, EntireTimeDomainSubset, TimeDomain> of(TimeRangeHolder value)
	{
		return value == null ? NullValue.instance(TIMEDS) : new GenericTimeValue<>(value, TIMEDS);
	}
	
	public static ScalarValue<?, ?, EntireTimeDomainSubset, TimeDomain> of(TimeValue<?, ?, ?, ?> start, TimeValue<?, ?, ?, ?> endInclusive)
	{
		return start == null || endInclusive == null ? NullValue.instance(TIMEDS) : new GenericTimeValue<>(new TimeRangeHolder(start, endInclusive), TIMEDS);
	}
	
	private static final long serialVersionUID = 1L;

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		return get().compareTo(((GenericTimeValue<?>) o).get());
	}

	@Override
	public GenericTimeValue<S> add(long amount)
	{
		return new GenericTimeValue<>(get().incrementSmallest(amount), getDomain());
	}

	@Override
	public GenericTimeValue<S> add(TemporalAmount period)
	{
		return new GenericTimeValue<>(get().increment(period), getDomain());
	}
	
	@Override
	public Period until(TimeValue<?, ?, ?, ?> endInclusive)
	{
		return get().until(endInclusive);
	}

	@Override
	public DurationValue getFrequency()
	{
		return get().getFrequency();
	}

	@Override
	public DateValue<?> getStartDate()
	{
		return get().getStartDate();
	}

	@Override
	public DateValue<?> getEndDate()
	{
		return get().getEndDate();
	}
}
