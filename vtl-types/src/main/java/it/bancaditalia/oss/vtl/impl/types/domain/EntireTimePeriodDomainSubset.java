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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.MONTHSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.QUARTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.SEMESTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.WEEKSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.YEARSDS;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class EntireTimePeriodDomainSubset<S extends EntireTimePeriodDomainSubset<S>> extends EntireDomainSubset<S, TimePeriodDomain> implements TimePeriodDomainSubset<S>
{
	private static final long serialVersionUID = 1L;
	
	public static class YearsDomainSubset extends EntireTimePeriodDomainSubset<YearsDomainSubset>
	{
		private static final long serialVersionUID = 1L;
		
		public YearsDomainSubset()
		{
			super(YEARSDS);
		}

		@Override
		protected YearPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new YearPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof MonthsDomainSubset || other instanceof QuartersDomainSubset || other instanceof SemestersDomainSubset || other instanceof YearsDomainSubset;
		}
	};
	
	public static class SemestersDomainSubset extends EntireTimePeriodDomainSubset<SemestersDomainSubset>
	{
		private static final long serialVersionUID = 1L;
		
		public SemestersDomainSubset()
		{
			super(SEMESTERSDS);
		}

		@Override
		protected SemesterPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new SemesterPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof MonthsDomainSubset || other instanceof QuartersDomainSubset || other instanceof SemestersDomainSubset;
		}
	};
	
	public static class QuartersDomainSubset extends EntireTimePeriodDomainSubset<QuartersDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public QuartersDomainSubset()
		{
			super(QUARTERSDS);
		}
		
		@Override
		protected QuarterPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new QuarterPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof MonthsDomainSubset || other instanceof QuartersDomainSubset;
		}
	};
	
	public static class MonthsDomainSubset extends EntireTimePeriodDomainSubset<MonthsDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public MonthsDomainSubset()
		{
			super(MONTHSDS);
		}
		
		@Override
		protected MonthPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new MonthPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof MonthsDomainSubset;
		}
	};

	public static class WeeksDomainSubset extends EntireTimePeriodDomainSubset<WeeksDomainSubset>
	{
		private static final long serialVersionUID = 1L;
		
		public WeeksDomainSubset()
		{
			super(WEEKSDS);
		}

		@Override
		protected WeekPeriodHolder getHolder(PeriodHolder<?> holder)
		{
			return new WeekPeriodHolder(holder);
		}

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return other instanceof WeeksDomainSubset;
		}
	};
	
	public EntireTimePeriodDomainSubset(TimePeriodDomain parentDomain)
	{
		super(parentDomain);
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

	protected PeriodHolder<?> getHolder(PeriodHolder<?> value)
	{
		throw new UnsupportedOperationException("Undetermined time period frequency.");
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof NullDomain || other instanceof TimePeriodDomainSubset; 
	}
	
	@Override
	public String toString()
	{
		return "time_period";
	}
}