/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.types.domain;

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoUnit.SEMESTERS;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.WEEKS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.time.temporal.IsoFields.QUARTER_YEARS;

import java.time.temporal.TemporalUnit;

import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

/**
 * Must be in period-length order 
 */
public enum Duration
{
	A(YEARS), S(SEMESTERS), Q(QUARTER_YEARS), M(MONTHS), W(WEEKS), D(DAYS);

	private final TemporalUnit unit;

	Duration(TemporalUnit unit)
	{
		this.unit = unit;
	}

	public TemporalUnit getUnit()
	{
		return unit;
	}

	public TimePeriodDomainSubset getDomain()
	{
		return EntireTimePeriodDomainSubset.of(this);
	}
}