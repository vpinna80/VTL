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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DAYSDS;
import static java.time.temporal.ChronoUnit.DAYS;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public class DayPeriodHolder extends PeriodHolder<DayPeriodHolder>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DayPeriodHolder.class);
	private static final long serialVersionUID = 1L;

	private final LocalDate date;

	public DayPeriodHolder(TemporalAccessor other)
	{
		this.date = LocalDate.from(other);
	}

	@Override
	public long getLong(TemporalField field)
	{
		return date.getLong(field);
	}
	
	@Override
	public boolean isSupported(TemporalField field)
	{
		return date.isSupported(field);
	}
	
	@Override
	public int compareTo(PeriodHolder<?> other)
	{
		int c = date.compareTo(LocalDate.from(other));
		LOGGER.trace("Comparing {} and {} yield {}.", date, other, c);
		return c;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
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
		DayPeriodHolder other = (DayPeriodHolder) obj;
		if (date == null)
		{
			if (other.date != null)
				return false;
		}
		else if (!date.equals(other.date))
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return DateTimeFormatter.ofPattern("yyyy'P'DDD").format(date);
	}

	@Override
	public boolean isSupported(TemporalUnit unit)
	{
		return date.isSupported(unit);
	}

	@Override
	public Temporal plus(long amount, TemporalUnit unit)
	{
		return new DayPeriodHolder(date.plus(amount, unit));
	}

	@Override
	protected TemporalUnit smallestUnit()
	{
		return DAYS;
	}

	@Override
	public TimePeriodDomainSubset<?> getDomain()
	{
		return DAYSDS;
	}
}
