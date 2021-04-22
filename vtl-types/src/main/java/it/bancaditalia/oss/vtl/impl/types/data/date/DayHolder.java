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

import java.time.LocalDate;
import java.time.temporal.TemporalField;

class DayHolder extends DateHolder<LocalDate>
{
	private static final long serialVersionUID = 1L;

	private final LocalDate date;

	public DayHolder(int year, int month, int day)
	{
		date = LocalDate.of(year, month, day);
	}
	
	public DayHolder(LocalDate date)
	{
		this.date = date;
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
	public int compareTo(DateHolder<?> other)
	{
		return date.compareTo(LocalDate.from(other));
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
		DayHolder other = (DayHolder) obj;
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
		return date.toString();
	}

	@Override
	public DayHolder increment(long amount)
	{
		return new DayHolder(date.plusDays(amount));
	}
}
