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

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import java.io.Serializable;
import java.time.temporal.TemporalAccessor;

import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;

public abstract class DateHolder<T extends TemporalAccessor> implements TemporalAccessor, Comparable<DateHolder<?>>, Serializable, TimeHolder
{
	private static final long serialVersionUID = 1L;

	public static final DateHolder<?> of(TemporalAccessor value)
	{
		if (value.isSupported(DAY_OF_MONTH))
			return new DayHolder(value.get(YEAR), value.get(MONTH_OF_YEAR), value.get(DAY_OF_MONTH));
		else if (value.isSupported(MONTH_OF_YEAR)) 
			return new MonthHolder(value.get(YEAR), value.get(MONTH_OF_YEAR));
		else if (value.isSupported(YEAR)) 
			return new YearHolder(value.get(YEAR));
		else
			throw new UnsupportedOperationException("Unsupported time fields in " + value);
	}
	
	@Override
	public abstract int hashCode();
	
	@Override
	public abstract boolean equals(Object obj);

	@Override
	public abstract String toString();

	@Override
	public TimePeriodValue wrap(DurationDomains frequency)
	{
		return new TimePeriodValue(wrapImpl(frequency));
	}

	protected abstract PeriodHolder<?> wrapImpl(DurationDomains frequency);

	public abstract DurationDomains getPeriod();

	public abstract DateHolder<?> increment(long amount);
}
