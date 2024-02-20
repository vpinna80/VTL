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
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;

import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;

public abstract class PeriodHolder<I extends PeriodHolder<I>> implements Temporal, Comparable<PeriodHolder<?>>, Serializable, TimeHolder
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

	public abstract ScalarValue<?, ?, ? extends DateDomainSubset<?>, ? extends DateDomain> startDate();

	public abstract ScalarValue<?, ?, ? extends DateDomainSubset<?>, ? extends DateDomain> endDate();

	public abstract DurationValue getPeriodIndicator();

	protected abstract TemporalUnit smallestUnit();
}