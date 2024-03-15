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

import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.WEEK_PERIOD_FORMATTER;

import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.YearWeek;

import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue.Duration;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;

public class WeekPeriodHolder extends PeriodHolder<WeekPeriodHolder>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(WeekPeriodHolder.class);
	private static final long serialVersionUID = 1L;

	private final YearWeek yearWeek;

	public WeekPeriodHolder(TemporalAccessor other)
	{
		this.yearWeek = YearWeek.from(other);
	}

	@Override
	public long getLong(TemporalField field)
	{
		return yearWeek.getLong(field);
	}
	
	@Override
	public boolean isSupported(TemporalField field)
	{
		return yearWeek.isSupported(field);
	}
	
	@Override
	public int compareTo(PeriodHolder<?> other)
	{
		int c = yearWeek.compareTo(YearWeek.from(other));
		LOGGER.trace("Comparing {} and {} yield {}.", this, other, c);
		return c;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((yearWeek == null) ? 0 : yearWeek.hashCode());
		return result;
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
		WeekPeriodHolder other = (WeekPeriodHolder) obj;
		if (yearWeek == null)
		{
			if (other.yearWeek != null)
				return false;
		}
		else if (!yearWeek.equals(other.yearWeek))
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return WEEK_PERIOD_FORMATTER.get().format(this);
	}

	@Override
	public ScalarValue<?, ?, ? extends DateDomainSubset<?>, ? extends DateDomain> startDate()
	{
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public ScalarValue<?, ?, ? extends DateDomainSubset<?>, ? extends DateDomain> endDate()
	{
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
	
	@Override
	public DurationValue getPeriodIndicator()
	{
		return Duration.W.get();
	}
}
