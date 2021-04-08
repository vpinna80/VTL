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

import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.DaysDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.MonthsDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.QuartersDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.SemestersDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.WeeksDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.YearsDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;

public class Domains<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements ScalarValueMetadata<S, D>
{
	public static final Domains<EntireBooleanDomainSubset, BooleanDomain> BOOLEAN = new Domains<>(new EntireBooleanDomainSubset());
	public static final Domains<EntireIntegerDomainSubset, IntegerDomain> INTEGER = new Domains<>(new EntireIntegerDomainSubset());
	public static final Domains<EntireDurationDomainSubset, DurationDomain> DURATION = new Domains<>(EntireDurationDomainSubset.INSTANCE);
	public static final Domains<EntireStringDomainSubset, StringDomain> STRING = new Domains<>(new EntireStringDomainSubset());
	public static final Domains<EntireNumberDomainSubset, ?> NUMBER = new Domains<>(new EntireNumberDomainSubset());
	public static final Domains<EntireTimeDomainSubset, TimeDomain> TIME = new Domains<>(EntireTimeDomainSubset.getInstance());
	public static final Domains<EntireDateDomainSubset, DateDomain> DATE = new Domains<>(EntireDateDomainSubset.getInstance());
	public static final Domains<DaysDomainSubset, TimePeriodDomain> DAYS = new Domains<>(new DaysDomainSubset());
	public static final Domains<WeeksDomainSubset, TimePeriodDomain> WEEKS = new Domains<>(new WeeksDomainSubset());
	public static final Domains<MonthsDomainSubset, TimePeriodDomain> MONTHS = new Domains<>(new MonthsDomainSubset());
	public static final Domains<QuartersDomainSubset, TimePeriodDomain> QUARTERS = new Domains<>(new QuartersDomainSubset());
	public static final Domains<SemestersDomainSubset, TimePeriodDomain> SEMESTERS = new Domains<>(new SemestersDomainSubset());
	public static final Domains<YearsDomainSubset, TimePeriodDomain> YEARS = new Domains<>(new YearsDomainSubset());

	public static final EntireDurationDomainSubset DURATIONDS = DURATION.getDomain();
	public static final EntireNumberDomainSubset NUMBERDS = NUMBER.getDomain();
	public static final EntireIntegerDomainSubset INTEGERDS = INTEGER.getDomain();
	public static final EntireBooleanDomainSubset BOOLEANDS = BOOLEAN.getDomain();
	public static final EntireStringDomainSubset STRINGDS = STRING.getDomain();
	public static final EntireDateDomainSubset DATEDS = DATE.getDomain();
	public static final EntireTimeDomainSubset TIMEDS = TIME.getDomain();
	public static final DaysDomainSubset DAYSDS = DAYS.getDomain();
	public static final WeeksDomainSubset WEEKSDS = WEEKS.getDomain();
	public static final MonthsDomainSubset MONTHSDS = MONTHS.getDomain();
	public static final QuartersDomainSubset QUARTERSDS = QUARTERS.getDomain();
	public static final SemestersDomainSubset SEMESTERSDS = SEMESTERS.getDomain();
	public static final YearsDomainSubset YEARSDS = YEARS.getDomain();
	public static final NullDomain UNKNOWNDS = new NullDomain();

	private final S valueDomain;
	
	private Domains(S valueDomain)
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

	public static class NullDomain implements ValueDomainSubset<NullDomain, ValueDomain> 
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
		public ScalarValue<?, ?, NullDomain, ValueDomain> cast(ScalarValue<?, ?, ?, ?> value)
		{
			throw new UnsupportedOperationException("Cast to unknown domain not supported.");
		}

		@Override
		public String getVarName()
		{
			throw new UnsupportedOperationException("No variable name for unknown domain.");
		}
	}

	@Override
	public S getDomain()
	{
		return valueDomain;
	};
}
