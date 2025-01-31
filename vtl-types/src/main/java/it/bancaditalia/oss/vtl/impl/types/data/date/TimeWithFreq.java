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