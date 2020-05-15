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

import static it.bancaditalia.oss.vtl.impl.types.domain.Duration.M;
import static java.time.temporal.ChronoUnit.MONTHS;

import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.domain.Duration;

public class YearMonthPeriodHolder extends PeriodHolder<YearMonthPeriodHolder>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(YearMonthHolder.class);
	private static final long serialVersionUID = 1L;

	private final YearMonth yearMonth;

	public YearMonthPeriodHolder(TemporalAccessor other)
	{
		super(M);
		this.yearMonth = YearMonth.from(other);
	}

	@Override
	public long getLong(TemporalField field)
	{
		return yearMonth.getLong(field);
	}
	
	@Override
	public boolean isSupported(TemporalField field)
	{
		return yearMonth.isSupported(field);
	}
	
	@Override
	public int compareTo(PeriodHolder<?> other)
	{
		int c = yearMonth.compareTo(YearMonth.from(other));
		LOGGER.trace("Comparing {} and {} yield {}.", yearMonth, other, c);
		return c;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((yearMonth == null) ? 0 : yearMonth.hashCode());
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
		YearMonthPeriodHolder other = (YearMonthPeriodHolder) obj;
		if (yearMonth == null)
		{
			if (other.yearMonth != null)
				return false;
		}
		else if (!yearMonth.equals(other.yearMonth))
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return yearMonth.toString();
	}

	@Override
	public TemporalUnit getPeriod()
	{
		return MONTHS;
	}

	@Override
	public PeriodHolder<?> wrap(Duration frequency)
	{
		switch (frequency)
		{
			case A: return new YearPeriodHolder(this);
			case S: return new YearSemesterPeriodHolder(this);
			case Q: return new YearQuarterPeriodHolder(this);
		default:
			throw new UnsupportedOperationException("Cannot wrap " + this + " with duration " + frequency + " or wrapping time_period not implemented"); 
		}
	}

	@Override
	public boolean isSupported(TemporalUnit unit)
	{
		return yearMonth.isSupported(unit);
	}

	@Override
	public Temporal plus(long amount, TemporalUnit unit)
	{
		return new YearMonthPeriodHolder(yearMonth.plus(amount, unit));
	}
}
