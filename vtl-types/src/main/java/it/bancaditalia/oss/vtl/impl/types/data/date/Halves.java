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

import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.time.temporal.IsoFields.QUARTER_YEARS;
import static org.threeten.extra.TemporalFields.HALF_YEARS;

import java.io.Serializable;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;

public class Halves implements TemporalAmount, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final int halves;
	
	private Halves(int halves)
	{
		this.halves = halves;
	}
	
	public static Halves of(int halves)
	{
		return new Halves(halves);
	}
	
	@Override
	public Temporal subtractFrom(Temporal temporal)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public List<TemporalUnit> getUnits()
	{
		return List.of(HALF_YEARS);
	}

	@Override
	public long get(TemporalUnit unit)
	{
		if (unit == YEARS)
			return halves / 2;
		if (unit == HALF_YEARS)
			return halves;
		if (unit == QUARTER_YEARS)
			return halves * 2;
		if (unit == MONTHS)
			return halves * 6;
		
		throw new UnsupportedTemporalTypeException("Unsupported unit " + unit);
	}

	@Override
	public Temporal addTo(Temporal temporal)
	{
		return temporal.plus(halves % 2, HALF_YEARS).plus(halves / 2, YEARS);
	}
}