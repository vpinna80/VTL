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

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoUnit.SEMESTERS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.QUARTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains.Q;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;
import static java.time.temporal.IsoFields.QUARTER_YEARS;

import java.time.Year;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;

import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public class QuarterPeriodHolder extends PeriodHolder<QuarterPeriodHolder>
{
	private static final long serialVersionUID = 1L;

	private final Year year;
	private final int quarter;

	public QuarterPeriodHolder(TemporalAccessor other)
	{
		this.year = Year.from(other);
		this.quarter = other.get(QUARTER_OF_YEAR);
	}

	public QuarterPeriodHolder(int year, long quarter)
	{
		this.year = Year.of((int) (year + (quarter - 1) / 4));
		this.quarter = (int) (quarter % 4);
	}

	@Override
	public long getLong(TemporalField field)
	{
		return QUARTER_OF_YEAR.equals(field) ? quarter : year.getLong(field);
	}

	@Override
	public boolean isSupported(TemporalField field)
	{
		return QUARTER_OF_YEAR.equals(field) || year.isSupported(field);
	}

	@Override
	public int compareTo(PeriodHolder<?> other)
	{
		int c = year.compareTo(Year.from(other));
		return c != 0 ? c : Long.compare(quarter, ((QuarterPeriodHolder) other).quarter);
	}

	@Override
	public String toString()
	{
		return year.toString() + "-Q" + quarter;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + quarter;
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
		QuarterPeriodHolder other = (QuarterPeriodHolder) obj;
		if (quarter != other.quarter)
			return false;
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
	public DurationDomains getPeriod()
	{
		return Q;
	}

	@Override
	public PeriodHolder<?> wrapImpl(DurationDomains frequency)
	{
		switch (frequency)
		{
			case A: return new YearPeriodHolder<>(this);
			case S: return new SemesterPeriodHolder(this);
		default:
			throw new UnsupportedOperationException("Cannot wrap " + this + " with duration " + frequency + " or wrapping time_period not implemented"); 
		}
	}

	@Override
	public boolean isSupported(TemporalUnit unit)
	{
		return QUARTER_YEARS.equals(unit) || SEMESTERS.equals(unit) || year.isSupported(unit);
	}

	@Override
	public Temporal plus(long amount, TemporalUnit unit)
	{
		if (QUARTER_YEARS.equals(unit))
			return new QuarterPeriodHolder(year.getValue(), quarter + amount);
		else if (SEMESTERS.equals(unit))
			return new QuarterPeriodHolder(year.getValue(), quarter + amount * 2);
		return null;
	}

	@Override
	public TimePeriodDomainSubset getDomain()
	{
		return QUARTERSDS;
	}

	@Override
	protected TemporalUnit smallestUnit()
	{
		return QUARTER_YEARS;
	}
}
