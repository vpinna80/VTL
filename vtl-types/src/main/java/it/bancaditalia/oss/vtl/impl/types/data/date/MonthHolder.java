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

import java.time.YearMonth;
import java.time.temporal.TemporalField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Valentino Pinna
 */
public class MonthHolder extends DateHolder<YearMonth>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(MonthHolder.class);
	private static final long serialVersionUID = 1L;

	private final YearMonth yearMonth;

	public MonthHolder(int year, int month)
	{
		this.yearMonth = YearMonth.of(year, month);
	}

	private MonthHolder(YearMonth yearMonth)
	{
		this.yearMonth = yearMonth;
	}

	@Override
	public long getLong(TemporalField field)
	{
		if (SEMESTER_OF_YEAR.equals(field))
			return (yearMonth.getMonthValue() - 1) / 6 + 1L;
		else
			return yearMonth.getLong(field);
	}
	
	@Override
	public boolean isSupported(TemporalField field)
	{
		return SEMESTER_OF_YEAR.equals(field) || yearMonth.isSupported(field);
	}
	
	@Override
	public int compareTo(DateHolder<?> other)
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
		MonthHolder other = (MonthHolder) obj;
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
	public MonthHolder increment(long amount)
	{
		return new MonthHolder(yearMonth.plusMonths(amount));
	}
}
