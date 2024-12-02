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
package it.bancaditalia.oss.vtl.impl.types.operators;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_YEAR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;

import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;

public enum TimeFieldOperator
{
	YEAR("year", ChronoField.YEAR), 
	MONTH("month", MONTH_OF_YEAR), 
	DAY_YEAR("dayofyear", DAY_OF_YEAR), 
	DAY_MONTH("dayofmonth", DAY_OF_MONTH);

	private final String name;
	private final TemporalField field;

	private TimeFieldOperator(String name, TemporalField field)
	{
		this.name = name;
		this.field = field;
	}
	
	@Override
	public String toString()
	{
		return name;
	}

	public TemporalField getField()
	{
		return field;
	}
}