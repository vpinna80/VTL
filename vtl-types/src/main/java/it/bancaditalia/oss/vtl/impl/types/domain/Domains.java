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

import it.bancaditalia.oss.vtl.impl.types.data.date.DayPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public enum Domains implements ScalarValueMetadata<ValueDomainSubset<?>>
{
	BOOLEAN(new EntireBooleanDomainSubset()),
	INTEGER(new EntireIntegerDomainSubset()),
	STRING(new EntireStringDomainSubset()),
	NUMBER(new EntireNumberDomainSubset()),
	DATE(new EntireDateDomainSubset()),
	TIME(EntireTimeDomainSubset.getInstance()), 
	DAYS(new EntireTimePeriodDomainSubset<>(DayPeriodHolder.class, "time_period_days_var")),
	WEEKS(new EntireTimePeriodDomainSubset<>(WeekPeriodHolder.class, "time_period_weeks_var")),
	MONTHS(new EntireTimePeriodDomainSubset<>(MonthPeriodHolder.class, "time_period_months_var")),
	QUARTERS(new EntireTimePeriodDomainSubset<>(QuarterPeriodHolder.class, "time_period_quarters_var")),
	SEMESTERS(new EntireTimePeriodDomainSubset<>(SemesterPeriodHolder.class, "time_period_semesters_var")),
	@SuppressWarnings({ "rawtypes", "unchecked" })
	YEARS(new EntireTimePeriodDomainSubset(YearPeriodHolder.clazz(), "time_period_years_var"));

	private final ValueDomainSubset<?> valueDomain;
	
	Domains(ValueDomainSubset<?> valueDomain)
	{
		this.valueDomain = valueDomain;
	}
	
	public ValueDomainSubset<?> getDomain()
	{
		return valueDomain;
	}
	
	public boolean isAssignableFrom(Domains other)
	{
		return getDomain().isAssignableFrom(other.getDomain());
	}

	public boolean isAssignableFrom(ValueDomainSubset<?> other)
	{
		return getDomain().isAssignableFrom(other);
	}

	public static class UnknownDomainSubset implements ValueDomainSubset<ValueDomain> 
	{
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return false;
		}

		@Override
		public boolean isComparableWith(ValueDomain other)
		{
			return false;
		}

		@Override
		public Object getCriterion()
		{
			return null;
		}

		@Override
		public ValueDomain getParentDomain()
		{
			return null;
		}
		
		@Override
		public ScalarValue<?, ? extends ValueDomainSubset<ValueDomain>, ValueDomain> cast(ScalarValue<?, ?, ?> value)
		{
			throw new UnsupportedOperationException("Cast to unknown domain not supported.");
		}

		@Override
		public String getVarName()
		{
			throw new UnsupportedOperationException("No variable name for unknown domain.");
		}
	};

	@SuppressWarnings("unchecked")
	public static final NumberDomainSubset<NumberDomain> NUMBERDS = (NumberDomainSubset<NumberDomain>) NUMBER.getDomain();
	public static final IntegerDomainSubset INTEGERDS = (IntegerDomainSubset) INTEGER.getDomain();
	public static final BooleanDomainSubset BOOLEANDS = (BooleanDomainSubset) BOOLEAN.getDomain();
	public static final StringDomainSubset STRINGDS = (StringDomainSubset) STRING.getDomain();
	public static final DateDomainSubset DATEDS = (DateDomainSubset) DATE.getDomain();
	public static final TimeDomainSubset<TimeDomain> TIMEDS = EntireTimeDomainSubset.getInstance();
	public static final TimePeriodDomainSubset DAYSDS = (TimePeriodDomainSubset) DAYS.getDomain();
	public static final TimePeriodDomainSubset WEEKSDS = (TimePeriodDomainSubset) WEEKS.getDomain();
	public static final TimePeriodDomainSubset MONTHSDS = (TimePeriodDomainSubset) MONTHS.getDomain();
	public static final TimePeriodDomainSubset QUARTERSDS = (TimePeriodDomainSubset) QUARTERS.getDomain();
	public static final TimePeriodDomainSubset SEMESTERSDS = (TimePeriodDomainSubset) SEMESTERS.getDomain();
	public static final TimePeriodDomainSubset YEARSDS = (TimePeriodDomainSubset) YEARS.getDomain();
	public static final ValueDomainSubset<? extends ValueDomain> UNKNOWNDS = new UnknownDomainSubset();
}
