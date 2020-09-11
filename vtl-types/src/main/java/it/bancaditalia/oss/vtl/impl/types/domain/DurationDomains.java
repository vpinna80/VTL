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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DAYSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.MONTHSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.QUARTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.SEMESTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.WEEKSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.YEARSDS;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.time.temporal.IsoFields.QUARTER_YEARS;

import java.time.temporal.TemporalUnit;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

/**
 * Must be in period-length order 
 */
public enum DurationDomains implements DurationDomainSubset
{
	A(YEARSDS, YEARS, null), S(SEMESTERSDS, SEMESTERS, A), Q(QUARTERSDS, QUARTER_YEARS, S), M(MONTHSDS, MONTHS, A), W(WEEKSDS, MONTHS, A), D(DAYSDS, DAYS, M);

	private final TimePeriodDomainSubset relatedTimePeriodDomain;
	private final TemporalUnit unit;
	private final DurationDomains parent;

	DurationDomains(TimePeriodDomainSubset relatedTimePeriod, TemporalUnit unit, DurationDomains parent)
	{
		this.relatedTimePeriodDomain = relatedTimePeriod;
		this.unit = unit;
		this.parent = parent;
	}

	public TimePeriodDomainSubset getRelatedTimePeriodDomain()
	{
		return relatedTimePeriodDomain;
	}

	public DurationDomains getParent()
	{
		return parent;
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public String getVarName()
	{
		return "duration_" + this.name() + "_var";
	}

	@Override
	public Object getCriterion()
	{
		return null;
	}

	@Override
	public DurationDomain getParentDomain()
	{
		return null;
	}

	@Override
	public ScalarValue<?, ? extends ValueDomainSubset<? extends DurationDomain>, ? extends DurationDomain> cast(ScalarValue<?, ?, ?> value)
	{
		throw new UnsupportedOperationException("Cast to duration domain");
	}
	
	@Override
	public String toString()
	{
		return "Duration(" + this.name() + ")";
	}

	public TemporalUnit getUnit()
	{
		return unit;
	}
}