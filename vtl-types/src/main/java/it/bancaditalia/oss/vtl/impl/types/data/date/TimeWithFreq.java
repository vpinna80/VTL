package it.bancaditalia.oss.vtl.impl.types.data.date;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.Period;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;

public final class TimeWithFreq implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Frequency[] FREQS = Frequency.values();
	
	public int freq = 0;
	public TimeRangeHolder range;
	public LocalDate date;
	public PeriodHolder<?> holder;

	public void setTime(TimeValue<?, ?, ?, ?> newTime)
	{
		Period p = null;
		if (range != null)
			p = range.until(newTime);
		else if (date != null)
			p = date.until(newTime.getEndDate().get());
		else if (holder != null)
			p = holder.until(newTime);

		if (p != null)
			for (int i = freq + 1; i < FREQS.length; i++)
				// Only allow inferred frequency to become smaller (i.e. from A to Q but not the opposite)
				if (FREQS[i].isMultiple(p))
				{
					freq = i++;
					break;
				}

		range = null;
		date = null;
		holder = null;
		if (newTime instanceof DateValue)
			date = (LocalDate) newTime.get();
		else if (newTime instanceof GenericTimeValue)
			range = (TimeRangeHolder) newTime.get();
		else if (newTime instanceof TimePeriodValue)
			holder = (PeriodHolder<?>) newTime.get();
	}
	
	public TimeWithFreq combine(TimeWithFreq other)
	{
		if (freq < other.freq)
			freq = other.freq;

		Period p = null;
		if (range != null)
			p = range.until((TimeValue<?, ?, ?, ?>) GenericTimeValue.of(other.range));
		else if (date != null)
			p = date.until(other.date);
		else if (holder != null)
			p = holder.until((TimeValue<?, ?, ?, ?>) TimePeriodValue.of(other.holder));

		if (p != null)
			for (int i = freq + 1; i < FREQS.length; i++)
				// Only allow inferred frequency to become smaller (i.e. from A to Q but not the opposite)
				if (FREQS[i].isMultiple(p))
				{
					freq = i++;
					break;
				}

		range = other.range;
		date = other.date;
		holder = other.holder;
		
		return this;
	}
	
	public DurationValue getDuration()
	{
		return FREQS[freq].get();
	}
}