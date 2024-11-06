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

import static it.bancaditalia.oss.vtl.impl.types.data.Frequency.Q;

import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;

import org.threeten.extra.YearQuarter;

import it.bancaditalia.oss.vtl.impl.types.data.Frequency;

public class QuarterPeriodHolder extends PeriodHolder<QuarterPeriodHolder>
{
	private static final long serialVersionUID = 1L;

	private final YearQuarter yearQtr;

	public QuarterPeriodHolder(TemporalAccessor other)
	{
		this.yearQtr = YearQuarter.from(other);
	}

	@Override
	public long getLong(TemporalField field)
	{
		return yearQtr.getLong(field);
	}

	@Override
	public boolean isSupported(TemporalField field)
	{
		return yearQtr.isSupported(field);
	}

	@Override
	public int compareTo(PeriodHolder<?> other)
	{
		return yearQtr.compareTo(YearQuarter.from(other));
	}

	@Override
	public String toString()
	{
		return yearQtr.toString();
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((yearQtr == null) ? 0 : yearQtr.hashCode());
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
		if (yearQtr == null)
		{
			if (other.yearQtr != null)
				return false;
		}
		else if (!yearQtr.equals(other.yearQtr))
			return false;
		return true;
	}

	@Override
	public LocalDate startDate()
	{
		return yearQtr.atDay(1);
	}

	@Override
	public LocalDate endDate()
	{
		return yearQtr.atEndOfQuarter();
	}

	@Override
	public Frequency getPeriodIndicator()
	{
		return Q;
	}
}
