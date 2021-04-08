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

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoField.SEMESTER_OF_YEAR;
import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoUnit.SEMESTERS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.SEMESTERSDS;

import java.time.Year;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;

import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public class SemesterPeriodHolder extends PeriodHolder<SemesterPeriodHolder>
{
	private static final long serialVersionUID = 1L;

	private final int semester;
	private final Year year;

	public SemesterPeriodHolder(TemporalAccessor other)
	{
		this.year = Year.from(other);
		this.semester = other.get(SEMESTER_OF_YEAR);
	}

	private SemesterPeriodHolder(Year year, int semester)
	{
		this.year = year;
		this.semester = semester;
	}

	@Override
	public long getLong(TemporalField field)
	{
		return SEMESTER_OF_YEAR.equals(field) ? semester : year.getLong(field);
	}

	@Override
	public boolean isSupported(TemporalField field)
	{
		return SEMESTER_OF_YEAR.equals(field) || year.isSupported(field);
	}

	@Override
	public int compareTo(PeriodHolder<?> other)
	{
		int c = year.compareTo(Year.from(other));
		return c != 0 ? c : Long.compare(semester, ((SemesterPeriodHolder) other).semester);
	}

	@Override
	public String toString()
	{
		return year.toString() + "-S" + semester;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + semester;
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
		SemesterPeriodHolder other = (SemesterPeriodHolder) obj;
		if (semester != other.semester)
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
	public boolean isSupported(TemporalUnit unit)
	{
		return SEMESTERS.equals(unit) || year.isSupported(unit);
	}

	@Override
	public Temporal plus(long amount, TemporalUnit unit)
	{
		if (SEMESTERS.equals(unit))
			return new SemesterPeriodHolder(year.plusYears((semester + amount) / 2), (int)(semester + amount) % 2);
		else
			return new SemesterPeriodHolder(year.plus(amount, unit), semester);
	}

	@Override
	protected TemporalUnit smallestUnit()
	{
		return SEMESTERS;
	}
	
	@Override
	public TimePeriodDomainSubset<?> getDomain()
	{
		return SEMESTERSDS;
	}
}
