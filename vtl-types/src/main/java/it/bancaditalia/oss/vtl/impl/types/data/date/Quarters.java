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

import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;
import static java.time.temporal.IsoFields.QUARTER_YEARS;
import static org.threeten.extra.TemporalFields.HALF_YEARS;

import java.io.Serializable;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;

import org.threeten.extra.YearQuarter;

public class Quarters implements TemporalAmount, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final int quarters;
	
	private Quarters(int quarters)
	{
		this.quarters = quarters;
	}
	
	public static Quarters of(int halves)
	{
		return new Quarters(halves);
	}
	
	@Override
	public Temporal subtractFrom(Temporal temporal)
	{
		if (temporal instanceof YearQuarter)
		{
			int newQuarter = temporal.get(QUARTER_OF_YEAR) - quarters;
			
			int leap = 0;
			for (; newQuarter < 1; newQuarter += 4)
				leap--;
			for (; newQuarter > 4; newQuarter -= 4)
				leap++;
			
			int newYear = temporal.get(YEAR) - quarters / 4 + leap;
			return YearQuarter.of(newYear, newQuarter);
		}
		else
			throw new UnsupportedOperationException();
	}

	@Override
	public List<TemporalUnit> getUnits()
	{
		return List.of(QUARTER_YEARS);
	}

	@Override
	public long get(TemporalUnit unit)
	{
		if (unit == YEARS)
			return quarters / 4;
		if (unit == HALF_YEARS)
			return quarters / 2;
		if (unit == QUARTER_YEARS)
			return quarters;
		if (unit == MONTHS)
			return quarters * 3;
		
		throw new UnsupportedTemporalTypeException("Unsupported unit " + unit);
	}

	@Override
	public Temporal addTo(Temporal temporal)
	{
		return temporal.plus(quarters % 4, QUARTER_YEARS).plus(quarters / 4, YEARS);
	}
}