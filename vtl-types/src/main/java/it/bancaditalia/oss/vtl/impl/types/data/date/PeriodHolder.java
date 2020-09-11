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
package it.bancaditalia.oss.vtl.impl.types.data.date;

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoField.SEMESTER_OF_YEAR;
import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;

import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public abstract class PeriodHolder<T extends PeriodHolder<? extends T>> implements Temporal, Comparable<PeriodHolder<?>>, Serializable, TimeHolder
{
	private static final long serialVersionUID = 1L;

	public enum Formatter implements Supplier<DateTimeFormatter>
	{
		YEAR_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)),
		SEMESTER_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.appendLiteral("-S")
				.appendValue(SEMESTER_OF_YEAR, 1)),
		QUARTER_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.appendLiteral("-Q")
				.appendValue(QUARTER_OF_YEAR, 1)),
		MONTH_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.appendLiteral("-")
				.appendValue(MONTH_OF_YEAR, 2)),
		WEEK_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.appendLiteral("-")
				.appendValue(ALIGNED_WEEK_OF_YEAR, 2)),
		DAY_OF_MONTH_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.appendLiteral("-")
				.appendValue(MONTH_OF_YEAR, 2)
				.appendLiteral("-")
				.appendValue(DAY_OF_MONTH, 2));

		private final DateTimeFormatter formatter;

		private Formatter(DateTimeFormatterBuilder builder)
		{
			this.formatter = builder.toFormatter();
		}

		public String format(TemporalAccessor value)
		{
			return formatter.format(value);
		}

		public TemporalAccessor parse(String value)
		{
			return formatter.parse(value);
		}
		
		@Override
		public DateTimeFormatter get()
		{
			return formatter;
		}
	}

	public static PeriodHolder<?> of(TemporalAccessor value)
	{
		if (value.isSupported(DAY_OF_MONTH)) 
			return new DayPeriodHolder(value);
		else if (value.isSupported(MONTH_OF_YEAR)) 
			return new MonthPeriodHolder(value);
		else if (value.isSupported(QUARTER_OF_YEAR)) 
			return new QuarterPeriodHolder(value);
		else if (value.isSupported(SEMESTER_OF_YEAR)) 
			return new SemesterPeriodHolder(value);
		else if (value.isSupported(ALIGNED_WEEK_OF_YEAR)) 
			return new WeekPeriodHolder(value);
		else if (value.isSupported(YEAR)) 
			return new YearPeriodHolder<>(value);
		else
			throw new UnsupportedOperationException("Period from " + value + " not implemented.");
	}

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
	
	@SuppressWarnings("unchecked")
	@Override
	public String getPeriodIndicator()
	{
		return getQualifier((Class<? extends PeriodHolder<?>>) getClass());
	}

	public abstract TimePeriodDomainSubset getDomain();

	public abstract DurationDomains getPeriod();

	@Override
	public TimePeriodValue wrap(DurationDomains frequency)
	{
		return new TimePeriodValue(wrapImpl(frequency));
	}

	protected abstract PeriodHolder<?> wrapImpl(DurationDomains frequency);

	public PeriodHolder<?> incrementSmallest(long amount)
	{
		try
		{
			return getClass().getConstructor(TemporalAccessor.class).newInstance(plus(amount, smallestUnit()));
		}
		catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e)
		{
			throw new IllegalStateException(e); // never occurs
		}
	}
	
	protected abstract TemporalUnit smallestUnit();

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object obj);

	@Override
	public abstract String toString();

	@Override
	public long until(Temporal endExclusive, TemporalUnit unit)
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Temporal with(TemporalField field, long newValue)
	{
		throw new UnsupportedOperationException();
	}
}