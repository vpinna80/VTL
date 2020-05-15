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
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;

import it.bancaditalia.oss.vtl.impl.types.domain.Duration;

public abstract class PeriodHolder<T extends PeriodHolder<? extends T>> implements Temporal, Comparable<PeriodHolder<?>>, Serializable
{
	private static final long serialVersionUID = 1L;

	public enum Formatter implements Supplier<DateTimeFormatter>
	{
		YEAR_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.toFormatter()),
		SEMESTER_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.appendLiteral("-S")
				.appendValue(SEMESTER_OF_YEAR, 1)
				.toFormatter()),
		QUARTER_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.appendLiteral("-Q")
				.appendValue(QUARTER_OF_YEAR, 1)
				.toFormatter()),
		MONTH_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.appendLiteral("-")
				.appendValue(MONTH_OF_YEAR, 2)
				.toFormatter());

		private final DateTimeFormatter formatter;

		private Formatter(DateTimeFormatter formatter)
		{
			this.formatter = formatter;
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

	private final Duration frequency;

	public PeriodHolder(Duration frequency)
	{
		this.frequency = frequency;
	}

	public static PeriodHolder<?> of(TemporalAccessor value)
	{
		if (value.isSupported(MONTH_OF_YEAR)) 
			return new YearMonthPeriodHolder(value);
		else if (value.isSupported(QUARTER_OF_YEAR)) 
			return new YearQuarterPeriodHolder(value);
		else if (value.isSupported(SEMESTER_OF_YEAR)) 
			return new YearSemesterPeriodHolder(value);
		else if (value.isSupported(YEAR)) 
			return new YearPeriodHolder(value);
		else
			throw new UnsupportedOperationException("Period from " + value + " not implemented.");
	}

	public Duration getFrequency()
	{
		return frequency;
	}

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object obj);

	@Override
	public abstract String toString();

	public abstract TemporalUnit getPeriod();

	public abstract PeriodHolder<?> wrap(Duration frequency);
	
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

	public PeriodHolder<?> incrementSmallest(long amount)
	{
		return (PeriodHolder<?>) plus(amount, frequency.getUnit());
	}

	public static Class<? extends PeriodHolder<?>> getImplementation(Duration duration)
	{
		switch (duration)
		{
			case A: return YearPeriodHolder.class;
			case S:	return YearSemesterPeriodHolder.class;
			case Q:	return YearQuarterPeriodHolder.class;
			case M:	return YearMonthPeriodHolder.class;
		default:
			return null;
		}
	}
}