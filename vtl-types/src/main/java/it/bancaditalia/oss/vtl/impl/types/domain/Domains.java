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
package it.bancaditalia.oss.vtl.impl.types.domain;

import java.util.EnumSet;

import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubsets.DaysDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubsets.MonthsDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubsets.QuartersDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubsets.SemestersDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubsets.WeeksDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubsets.YearsDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

@SuppressWarnings("rawtypes")
public enum Domains implements ScalarValueMetadata
{
	BOOLEAN(new EntireBooleanDomainSubset()),
	INTEGER(new EntireIntegerDomainSubset()),
	DURATION(EntireDurationDomainSubset.INSTANCE),
	STRING(new EntireStringDomainSubset()),
	NUMBER(new EntireNumberDomainSubset()),
	TIME(EntireTimeDomainSubset.getInstance()),
	DATE(EntireDateDomainSubset.getInstance()),
	TIME_PERIOD_DAYS(new DaysDomainSubset()),
	TIME_PERIOD_WEEKS(new WeeksDomainSubset()),
	TIME_PERIOD_MONTHS(new MonthsDomainSubset()),
	TIME_PERIOD_QUARTERS(new QuartersDomainSubset()),
	TIME_PERIOD_SEMESTERS(new SemestersDomainSubset()),
	TIME_PERIOD_YEARS(new YearsDomainSubset()),
	NULL(new NullDomain());
	
	public static final EnumSet<Domains> TIME_PERIODS = EnumSet.of(TIME_PERIOD_DAYS, TIME_PERIOD_WEEKS, TIME_PERIOD_MONTHS, TIME_PERIOD_QUARTERS, TIME_PERIOD_SEMESTERS, TIME_PERIOD_YEARS);

	public static final EntireDurationDomainSubset DURATIONDS = (EntireDurationDomainSubset) DURATION.getDomain();
	public static final EntireNumberDomainSubset NUMBERDS = (EntireNumberDomainSubset) NUMBER.getDomain();
	public static final EntireIntegerDomainSubset INTEGERDS = (EntireIntegerDomainSubset) INTEGER.getDomain();
	public static final EntireBooleanDomainSubset BOOLEANDS = (EntireBooleanDomainSubset) BOOLEAN.getDomain();
	public static final EntireStringDomainSubset STRINGDS = (EntireStringDomainSubset) STRING.getDomain();
	public static final EntireDateDomainSubset DATEDS = (EntireDateDomainSubset) DATE.getDomain();
	public static final EntireTimeDomainSubset TIMEDS = (EntireTimeDomainSubset) TIME.getDomain();
	public static final DaysDomainSubset DAYSDS = (DaysDomainSubset) TIME_PERIOD_DAYS.getDomain();
	public static final WeeksDomainSubset WEEKSDS = (WeeksDomainSubset) TIME_PERIOD_WEEKS.getDomain();
	public static final MonthsDomainSubset MONTHSDS = (MonthsDomainSubset) TIME_PERIOD_MONTHS.getDomain();
	public static final QuartersDomainSubset QUARTERSDS = (QuartersDomainSubset) TIME_PERIOD_QUARTERS.getDomain();
	public static final SemestersDomainSubset SEMESTERSDS = (SemestersDomainSubset) TIME_PERIOD_SEMESTERS.getDomain();
	public static final YearsDomainSubset YEARSDS = (YearsDomainSubset) TIME_PERIOD_YEARS.getDomain();
	public static final NullDomain NULLDS = (NullDomain) NULL.getDomain();

	private final ValueDomainSubset<?, ? extends ValueDomain> valueDomain;
	
	private Domains(ValueDomainSubset<?, ?> valueDomain)
	{
		this.valueDomain = valueDomain;
	}
	
	public boolean isAssignableFrom(ScalarValueMetadata<?, ?> other)
	{
		return getDomain().isAssignableFrom(other.getDomain());
	}

	public boolean isAssignableFrom(ValueDomainSubset<?, ?> other)
	{
		return getDomain().isAssignableFrom(other);
	}

	public ValueDomainSubset<?, ?> getDomain()
	{
		return valueDomain;
	}
}
