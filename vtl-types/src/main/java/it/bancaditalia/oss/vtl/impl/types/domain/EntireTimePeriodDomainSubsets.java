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
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DayPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public abstract class EntireTimePeriodDomainSubsets<S extends EntireTimePeriodDomainSubsets<S>> extends EntireDomainSubset<S, TimePeriodDomain> implements TimePeriodDomainSubset<S>
{
	private static final long serialVersionUID = 1L;
	
	public EntireTimePeriodDomainSubsets()
	{
		super(null, "time_period_var");
	}

	public static class AnyPeriodDomainSubset extends EntireTimePeriodDomainSubsets<AnyPeriodDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		@Override
		protected PeriodHolder<?> getHolder(PeriodHolder<?> holder)
		{
			return holder;
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof EntireTimePeriodDomainSubsets;
		}
	}
	
	public static class YearsDomainSubset extends EntireTimePeriodDomainSubsets<YearsDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		@Override
		protected YearPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new YearPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof DaysDomainSubset || other instanceof MonthsDomainSubset || other instanceof QuartersDomainSubset || other instanceof SemestersDomainSubset || other instanceof YearsDomainSubset;
		}
	};
	
	public static class SemestersDomainSubset extends EntireTimePeriodDomainSubsets<SemestersDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		@Override
		protected SemesterPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new SemesterPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof DaysDomainSubset || other instanceof MonthsDomainSubset || other instanceof QuartersDomainSubset || other instanceof SemestersDomainSubset;
		}
	};
	
	public static class QuartersDomainSubset extends EntireTimePeriodDomainSubsets<QuartersDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		@Override
		protected QuarterPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new QuarterPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof DaysDomainSubset || other instanceof MonthsDomainSubset || other instanceof QuartersDomainSubset;
		}
	};
	
	public static class MonthsDomainSubset extends EntireTimePeriodDomainSubsets<MonthsDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		@Override
		protected MonthPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new MonthPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof DaysDomainSubset || other instanceof MonthsDomainSubset;
		}
	};

	public static class WeeksDomainSubset extends EntireTimePeriodDomainSubsets<WeeksDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		@Override
		protected WeekPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new WeekPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof DaysDomainSubset || other instanceof WeeksDomainSubset;
		}
	};

	public static class DaysDomainSubset extends EntireTimePeriodDomainSubsets<DaysDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		@Override
		protected DayPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new DayPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof DaysDomainSubset;
		}
	};
	
	public EntireTimePeriodDomainSubsets(TimePeriodDomain parentDomain, String defaultVarName)
	{
		super(parentDomain, defaultVarName);
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	public ScalarValue<?, ?, S, TimePeriodDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof NullValue)
			return NullValue.instance((S) this);
		else if (value.getDomain() == this)
			return (ScalarValue<?, ?, S, TimePeriodDomain>) value;
		else if (value instanceof TimePeriodValue)
			return new TimePeriodValue<>(getHolder((PeriodHolder<?>) value.get()), (S) this);
		else
			throw new VTLCastException(this, value);
	}

	protected abstract PeriodHolder<?> getHolder(PeriodHolder<?> value);
	
	@Override
	public String toString()
	{
		return "time_period";
	}
}