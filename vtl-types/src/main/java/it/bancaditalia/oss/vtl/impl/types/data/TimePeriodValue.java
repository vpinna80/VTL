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
package it.bancaditalia.oss.vtl.impl.types.data;

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoField.SEMESTER_OF_YEAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.MONTHSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.QUARTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.SEMESTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.WEEKSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.YEARSDS;
import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;

import java.time.temporal.TemporalAccessor;

import it.bancaditalia.oss.vtl.impl.types.data.date.DayPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.DaysDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.MonthsDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.QuartersDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.SemestersDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.WeeksDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.TimePeriodDomainSubsets.YearsDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public abstract class TimePeriodValue<R extends PeriodHolder<R>, S extends TimePeriodDomainSubset<S>> extends TimeValue<TimePeriodValue<R, S>, R, S, TimePeriodDomain>
{
	private static final long serialVersionUID = 1L;

	public static TimePeriodValue<? extends PeriodHolder<?>, ? extends TimePeriodDomainSubset<?>> of(TemporalAccessor value)
	{
		if (value.isSupported(DAY_OF_MONTH)) 
			return new DayPeriodValue(new DayPeriodHolder(value));
		else if (value.isSupported(MONTH_OF_YEAR)) 
			return new MonthPeriodValue(new MonthPeriodHolder(value));
		else if (value.isSupported(QUARTER_OF_YEAR)) 
			return new QuarterPeriodValue(new QuarterPeriodHolder(value));
		else if (value.isSupported(SEMESTER_OF_YEAR)) 
			return new SemesterPeriodValue(new SemesterPeriodHolder(value));
		else if (value.isSupported(ALIGNED_WEEK_OF_YEAR)) 
			return new WeekPeriodValue(new WeekPeriodHolder(value));
		else if (value.isSupported(YEAR)) 
			return new YearPeriodValue(new YearPeriodHolder(value));
		else
			throw new UnsupportedOperationException("Period from " + value + " not implemented.");
	}
	
	public static class YearPeriodValue extends TimePeriodValue<YearPeriodHolder, YearsDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public YearPeriodValue(YearPeriodHolder value)
		{
			super(value, YEARSDS);
		}

		public static YearPeriodValue of(TemporalAccessor value)
		{
			return new YearPeriodValue(new YearPeriodHolder(value));
		}

		public YearPeriodValue increment(long amount)
		{
			return new YearPeriodValue(get().incrementSmallest(amount));
		}

		@Override
		public String getPeriodIndicator()
		{
			return "P1Y";
		}
	}

	public static class SemesterPeriodValue extends TimePeriodValue<SemesterPeriodHolder, SemestersDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public SemesterPeriodValue(SemesterPeriodHolder value)
		{
			super(value, SEMESTERSDS);
		}

		public static SemesterPeriodValue of(TemporalAccessor value)
		{
			return new SemesterPeriodValue(new SemesterPeriodHolder(value));
		}

		public SemesterPeriodValue increment(long amount)
		{
			return new SemesterPeriodValue(get().incrementSmallest(amount));
		}

		@Override
		public String getPeriodIndicator()
		{
			return "P1H";
		}
	}

	public static class QuarterPeriodValue extends TimePeriodValue<QuarterPeriodHolder, QuartersDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public QuarterPeriodValue(QuarterPeriodHolder value)
		{
			super(value, QUARTERSDS);
		}

		public static QuarterPeriodValue of(TemporalAccessor value)
		{
			return new QuarterPeriodValue(new QuarterPeriodHolder(value));
		}

		public QuarterPeriodValue increment(long amount)
		{
			return new QuarterPeriodValue(get().incrementSmallest(amount));
		}

		@Override
		public String getPeriodIndicator()
		{
			return "P1Q";
		}
	}

	public static class MonthPeriodValue extends TimePeriodValue<MonthPeriodHolder, MonthsDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public MonthPeriodValue(MonthPeriodHolder value)
		{
			super(value, MONTHSDS);
		}

		public static MonthPeriodValue of(TemporalAccessor value)
		{
			return new MonthPeriodValue(new MonthPeriodHolder(value));
		}
	
		public MonthPeriodValue increment(long amount)
		{
			return new MonthPeriodValue(get().incrementSmallest(amount));
		}

		@Override
		public String getPeriodIndicator()
		{
			return "P1M";
		}
	}

	public static class WeekPeriodValue extends TimePeriodValue<WeekPeriodHolder, WeeksDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public WeekPeriodValue(WeekPeriodHolder value)
		{
			super(value, WEEKSDS);
		}

		public static WeekPeriodValue of(TemporalAccessor value)
		{
			return new WeekPeriodValue(new WeekPeriodHolder(value));
		}

		public WeekPeriodValue increment(long amount)
		{
			return new WeekPeriodValue(get().incrementSmallest(amount));
		}

		@Override
		public String getPeriodIndicator()
		{
			return "P1W";
		}
	}

	public static class DayPeriodValue extends TimePeriodValue<DayPeriodHolder, DaysDomainSubset>
	{
		private static final long serialVersionUID = 1L;

		public DayPeriodValue(DayPeriodHolder value)
		{
			super(value, Domains.DAYSDS);
		}

		public static DayPeriodValue of(TemporalAccessor value)
		{
			return new DayPeriodValue(new DayPeriodHolder(value));
		}

		public DayPeriodValue increment(long amount)
		{
			return new DayPeriodValue(get().incrementSmallest(amount));
		}

		@Override
		public String getPeriodIndicator()
		{
			return "P1D";
		}
	}

	public TimePeriodValue(R value, S domain)
	{
		super(value, domain);
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof TimePeriodValue)
			return this.get().compareTo(((TimePeriodValue<?, ?>) o).get());
		else
			throw new VTLIncompatibleTypesException("compare", getDomain(), o.getDomain());
	}

	@Override
	public abstract TimePeriodValue<R, S> increment(long amount);
	
	public static String getQualifier(Class<? extends PeriodHolder<?>> holder)
	{
		if (DayPeriodHolder.class.isAssignableFrom(holder))
			return "P1D";
		if (WeekPeriodHolder.class.isAssignableFrom(holder))
			return "P1W";
		if (MonthPeriodHolder.class.isAssignableFrom(holder))
			return "P1M";
		if (QuarterPeriodHolder.class.isAssignableFrom(holder))
			return "P1Q";
		if (SemesterPeriodHolder.class.isAssignableFrom(holder))
			return "P1S";
		if (YearPeriodHolder.class.isAssignableFrom(holder))
			return "P1Y";
		throw new UnsupportedOperationException("Unknown class " + holder);
	}
	
	public abstract String getPeriodIndicator();
}
