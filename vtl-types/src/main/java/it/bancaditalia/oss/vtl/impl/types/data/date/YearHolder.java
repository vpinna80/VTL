/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.types.data.date;

import static java.time.temporal.ChronoUnit.YEARS;

import java.io.Serializable;
import java.time.Year;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.domain.Duration;

class YearHolder extends DateHolder<Year> implements Serializable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(YearHolder.class);
	private static final long serialVersionUID = 1L;

	private final Year year;

	public YearHolder(int year)
	{
		this.year = Year.of(year);
	}

	protected YearHolder(Year year)
	{
		this.year = year;
	}

	@Override
	public long getLong(TemporalField field)
	{
		return year.getLong(field);
	}

	@Override
	public boolean isSupported(TemporalField field)
	{
		return year.isSupported(field);
	}

	@Override
	public int compareTo(DateHolder<?> other)
	{
		int c = year.compareTo(Year.from(other));
		LOGGER.trace("Comparing {} and {} yield {}.", year, other, c);
		return c;
	}

	@Override
	public String toString()
	{
		return year.toString();
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((year == null) ? 0 : year.hashCode());
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
		YearHolder other = (YearHolder) obj;
		if (year == null)
		{
			if (other.year != null)
				return false;
		}
		else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public TemporalUnit getPeriod()
	{
		return YEARS;
	}

	@Override
	public YearHolder increment(long amount)
	{
		return new YearHolder(year.plusYears(amount));
	}

	@Override
	public PeriodHolder<?> wrap(Duration frequency)
	{
		switch (frequency)
		{
			case A: return new YearPeriodHolder(this);
			default:
				throw new UnsupportedOperationException("Cannot wrap " + this + " with duration " + frequency + " or wrapping time_period not implemented"); 
		}
	}
}
