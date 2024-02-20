package it.bancaditalia.oss.vtl.impl.types.data;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import java.io.Serializable;
import java.time.temporal.TemporalAccessor;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue.Duration;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDurationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerSupplier;

public class DurationValue extends BaseScalarValue<DurationValue, Duration, EntireDurationDomainSubset, DurationDomain> implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public enum Duration implements SerSupplier<DurationValue>
	{
		D(null), W(WeekPeriodHolder::new), M(MonthPeriodHolder::new), Q(QuarterPeriodHolder::new), S(SemesterPeriodHolder::new), A(YearPeriodHolder::new);
		
		private final DurationValue value;
		private final SerFunction<TemporalAccessor, ? extends PeriodHolder<?>> holderAllocator;
		
		private Duration(SerFunction<TemporalAccessor, ? extends PeriodHolder<?>> holderAllocator)
		{
			this.holderAllocator = holderAllocator;
			value = new DurationValue(this);
		}

		public DurationValue get()
		{
			return value;
		}
		
		public TimePeriodValue<EntireTimePeriodDomainSubset> wrap(TimeValue<?, ?, ?, ?> toWrap)
		{
			if (toWrap instanceof TimePeriodValue || toWrap instanceof DateValue)
			{
				TemporalAccessor accessor = ((TemporalAccessor) toWrap.get());
				return TimePeriodValue.of(holderAllocator.apply(accessor));
			}
			else
				throw new VTLCastException(TIMEDS, toWrap);
			 
		}
	}
	
	private DurationValue(Duration value)
	{
		super(value, DURATIONDS);
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof DurationValue)
			return get().compareTo(((DurationValue) o).get());
		else
			throw new VTLCastException(DURATIONDS, o);
	}
}
