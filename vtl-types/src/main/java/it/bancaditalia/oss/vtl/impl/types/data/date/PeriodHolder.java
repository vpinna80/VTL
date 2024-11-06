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
package it.bancaditalia.oss.vtl.impl.types.data.date;

import static java.time.format.SignStyle.NOT_NEGATIVE;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;
import static org.threeten.extra.TemporalFields.HALF_OF_YEAR;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.function.Supplier;

import org.threeten.extra.YearHalf;
import org.threeten.extra.YearQuarter;
import org.threeten.extra.YearWeek;

import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.TimeHolder;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;

public abstract class PeriodHolder<I extends PeriodHolder<I>> implements TemporalAccessor, Comparable<PeriodHolder<?>>, Serializable, TimeHolder
{
	private static final long serialVersionUID = 1L;

	public enum Formatter implements Supplier<DateTimeFormatter>
	{
		YEAR_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)),
		SEMESTER_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.optionalStart()
				.appendLiteral("-")
				.optionalEnd()
				.appendLiteral("S")
				.appendValue(HALF_OF_YEAR, 1)),
		QUARTER_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.optionalStart()
				.appendLiteral("-")
				.optionalEnd()
				.appendLiteral("Q")
				.appendValue(QUARTER_OF_YEAR, 1)),
		MONTH_PERIOD_FORMATTER(new DateTimeFormatterBuilder()
				.appendValue(YEAR, 4)
				.optionalStart()
				.appendLiteral("-")
				.optionalEnd()
				.appendLiteral("M")
				.appendValue(MONTH_OF_YEAR, 1, 2, NOT_NEGATIVE));

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

	public PeriodHolder<?> incrementSmallest(long amount)
	{
		if (this instanceof YearPeriodHolder)
			return new YearPeriodHolder(Year.from(this).plusYears(amount));
		else if (this instanceof SemesterPeriodHolder)
			return new SemesterPeriodHolder(YearHalf.from(this).plusHalves(amount));
		else if (this instanceof QuarterPeriodHolder)
			return new QuarterPeriodHolder(YearQuarter.from(this).plusQuarters(amount));
		else if (this instanceof MonthPeriodHolder)
			return new MonthPeriodHolder(YearMonth.from(this).plusMonths(amount));
		else if (this instanceof WeekPeriodHolder)
			return new WeekPeriodHolder(YearWeek.from(this).plusWeeks(amount));
		else
			throw new UnsupportedOperationException("Invalid PeriodHolder: " + this); 
	}

	public PeriodHolder<?> increment(TemporalAmount period)
	{
		if (this instanceof YearPeriodHolder)
			return new YearPeriodHolder(Year.from(this).plus(period));
		else if (this instanceof SemesterPeriodHolder)
			return new SemesterPeriodHolder(YearHalf.from(this).plus(period));
		else if (this instanceof QuarterPeriodHolder)
			return new QuarterPeriodHolder(YearQuarter.from(this).plus(period));
		else if (this instanceof MonthPeriodHolder)
			return new MonthPeriodHolder(YearMonth.from(this).plus(period));
		else if (this instanceof WeekPeriodHolder)
			return new WeekPeriodHolder(YearWeek.from(this).plus(period));
		else
			throw new UnsupportedOperationException("Invalid PeriodHolder: " + this); 
	}

	public Period until(TimeValue<?, ?, ?, ?> end)
	{
		return Period.between(startDate(), ((TimePeriodValue<?>) end).get().startDate());
	}

	public abstract LocalDate startDate();

	public abstract LocalDate endDate();

	public abstract Frequency getPeriodIndicator();
}