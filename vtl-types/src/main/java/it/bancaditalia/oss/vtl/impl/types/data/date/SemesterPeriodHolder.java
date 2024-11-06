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

import static it.bancaditalia.oss.vtl.impl.types.data.Frequency.S;

import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Objects;

import org.threeten.extra.YearHalf;

import it.bancaditalia.oss.vtl.impl.types.data.Frequency;

public class SemesterPeriodHolder extends PeriodHolder<SemesterPeriodHolder>
{
	private static final long serialVersionUID = 1L;

	private final YearHalf yearHalf;

	public SemesterPeriodHolder(TemporalAccessor other)
	{
		this.yearHalf = YearHalf.from(other);
	}

	@Override
	public long getLong(TemporalField field)
	{
		return yearHalf.getLong(field);
	}

	@Override
	public boolean isSupported(TemporalField field)
	{
		return yearHalf.isSupported(field);
	}

	@Override
	public int compareTo(PeriodHolder<?> other)
	{
		return yearHalf.compareTo(YearHalf.from(other));
	}

	@Override
	public String toString()
	{
		return yearHalf.toString();
	}

	@Override
	public int hashCode()
	{
		return yearHalf.hashCode();
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
		return Objects.equals(yearHalf, other.yearHalf);
	}

	@Override
	public LocalDate startDate()
	{
		return yearHalf.atDay(1);
	}

	@Override
	public LocalDate endDate()
	{
		return yearHalf.atEndOfHalf();
	}
	
	@Override
	public Frequency getPeriodIndicator()
	{
		return S;
	}
}
