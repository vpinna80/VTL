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

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.DayPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.MonthPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.QuarterPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.SemesterPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.WeekPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.YearPeriodValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public abstract class TimePeriodDomainSubsets<S extends TimePeriodDomainSubsets<S>> extends EntireDomainSubset<S, TimePeriodDomain> implements TimePeriodDomainSubset<S>
{
	private static final long serialVersionUID = 1L;

	public static class YearsDomainSubset extends TimePeriodDomainSubsets<YearsDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public YearsDomainSubset()
		{
			super(null, "time_period_years_var");
		}

		protected YearPeriodValue cast2(TimePeriodValue<?, ?> value)
		{
			return YearPeriodValue.of(value.get());
		}
	};
	
	public static class SemestersDomainSubset extends TimePeriodDomainSubsets<SemestersDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public SemestersDomainSubset()
		{
			super(null, "time_period_semesters_var");
		}

		protected SemesterPeriodValue cast2(TimePeriodValue<?, ?> value)
		{
			return SemesterPeriodValue.of(value.get());
		}
	};
	
	public static class QuartersDomainSubset extends TimePeriodDomainSubsets<QuartersDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public QuartersDomainSubset()
		{
			super(null, "time_period_quarters_var");
		}

		protected QuarterPeriodValue cast2(TimePeriodValue<?, ?> value)
		{
			return QuarterPeriodValue.of(value.get());
		}
	};
	
	public static class MonthsDomainSubset extends TimePeriodDomainSubsets<MonthsDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public MonthsDomainSubset()
		{
			super(null, "time_period_months_var");
		}

		protected MonthPeriodValue cast2(TimePeriodValue<?, ?> value)
		{
			return MonthPeriodValue.of(value.get());
		}
	};

	public static class WeeksDomainSubset extends TimePeriodDomainSubsets<WeeksDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public WeeksDomainSubset()
		{
			super(null, "time_period_weeks_var");
		}

		protected WeekPeriodValue cast2(TimePeriodValue<?, ?> value)
		{
			return WeekPeriodValue.of(value.get());
		}
	};

	public static class DaysDomainSubset extends TimePeriodDomainSubsets<DaysDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public DaysDomainSubset()
		{
			super(null, "time_period_days_var");
		}

		protected DayPeriodValue cast2(TimePeriodValue<?, ?> value)
		{
			return DayPeriodValue.of(value.get());
		}
	};
	
	public TimePeriodDomainSubsets(TimePeriodDomain parentDomain, String defaultVarName)
	{
		super(parentDomain, defaultVarName);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return this == other;
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		throw new UnsupportedOperationException();
	}

	public ScalarValue<?, ?, S, TimePeriodDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof TimePeriodValue && getClass().isInstance(value.getDomain()))
			return cast2((TimePeriodValue<?, ?>) value);
		else
			throw new VTLCastException(this, value);
	}

	protected abstract TimePeriodValue<?, S> cast2(TimePeriodValue<?, ?> value);
}