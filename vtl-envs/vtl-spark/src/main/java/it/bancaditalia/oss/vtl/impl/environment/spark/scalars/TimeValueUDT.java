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
package it.bancaditalia.oss.vtl.impl.environment.spark.scalars;

import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.threeten.extra.TemporalFields.HALF_OF_YEAR;

import java.security.InvalidParameterException;
import java.time.LocalDate;
import java.time.Year;
import java.time.YearMonth;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.threeten.extra.YearHalf;
import org.threeten.extra.YearQuarter;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;

public class TimeValueUDT extends ScalarValueUDT<TimeValue<?, ?, ?, ?>>
{
	private static final long serialVersionUID = 1L;
	private static final StructType SQL_TYPE = createStructType(List.of(
		createStructField("timetype", IntegerType, false),
		createStructField("startdate", LongType, true),
		createStructField("startyear", IntegerType, true),
		createStructField("startfreq", IntegerType, true),
		createStructField("startsubyear", LongType, true),
		createStructField("enddate", LongType, true),
		createStructField("endyear", IntegerType, true),
		createStructField("endfreq", IntegerType, true),
		createStructField("endsubyear", LongType, true)
	));

	public TimeValueUDT()
	{
		super(SQL_TYPE);
	}

	@Override
	protected TimeValue<?, ?, ?, ?> deserializeFrom(InternalRow row, int start)
	{
		int type = row.getInt(0);
		DateValue<?> startDate = null;
		TimePeriodValue<?> startPeriod = null;
		if (type == 0 || type == 2)
			startDate = (DateValue<?>) DateValue.of(LocalDate.ofEpochDay(row.getLong(start + 1)));
		else
		{
			PeriodHolder<?> holder;
			int year = row.getInt(start + 2);
			
			switch (row.getInt(start + 3))
			{
				case 0: holder = new YearPeriodHolder(Year.of(year)); break;
				case 1: holder = new SemesterPeriodHolder(YearHalf.of(year, (int) row.getLong(start + 4))); break;
				case 2: holder = new QuarterPeriodHolder(YearQuarter.of(year, (int) row.getLong(start + 4))); break;
				case 3: holder = new MonthPeriodHolder(YearMonth.of(year, (int) row.getLong(start + 4))); break;
				default: throw new InvalidParameterException("deserialize - bad period");
			};
			startPeriod = TimePeriodValue.of(holder);
		}
			
		if (type == 0)
			return startDate;
		else if (type == 1)
			return startPeriod;
		else if (type == 2)
			return (TimeValue<?, ?, ?, ?>) GenericTimeValue.of(new TimeRangeHolder(startDate, (DateValue<?>) DateValue.of(LocalDate.ofEpochDay(row.getLong(start + 5)))));
		else
		{
			PeriodHolder<?> holder;
			int year = row.getInt(start + 6);
			
			switch (row.getInt(start + 7))
			{
				case 0: holder = new YearPeriodHolder(Year.of(year)); break;
				case 1: holder = new SemesterPeriodHolder(YearHalf.of(year, (int) row.getLong(start + 8))); break;
				case 2: holder = new QuarterPeriodHolder(YearQuarter.of(year, (int) row.getLong(start + 8))); break;
				case 3: holder = new MonthPeriodHolder(YearMonth.of(year, (int) row.getLong(start + 8))); break;
				default: throw new InvalidParameterException("deserialize - bad period");
			};
			return (TimeValue<?, ?, ?, ?>) GenericTimeValue.of(new TimeRangeHolder(startPeriod, TimePeriodValue.of(holder)));
		}
	}

	@Override
	protected void serializeTo(TimeValue<?, ?, ?, ?> obj, InternalRow row, int start)
	{
		int type;
		if (obj.getClass() == DateValue.class)
			type = 0;
		else if (obj.getClass() == TimePeriodValue.class)
			type = 1;
		else if (((TimeRangeHolder) obj.get()).getStartTime().getClass() == DateValue.class)
			type = 2;
		else if (((TimeRangeHolder) obj.get()).getStartTime().getClass() == TimePeriodValue.class)
			type = 3;
		else
			throw new IllegalStateException();
		row.setInt(0, type);
		
		if (type == 0 || type == 2)
			row.setLong(start + 1, obj.getStartDate().get().toEpochDay());
		else if (type == 1 || type == 3)
		{
			PeriodHolder<?> holder = type == 1 ? ((TimePeriodValue<?>) obj).get() : (PeriodHolder<?>) ((GenericTimeValue<?>) obj).get().getStartTime().get();
			Class<?> objClass = holder.getClass();
			
			row.setInt(start + 2, holder.get(YEAR));
			if (objClass == YearPeriodHolder.class)
				row.setInt(start + 3, 0);
			else if (objClass == SemesterPeriodHolder.class)
			{
				row.setInt(start + 3, 1);
				row.setLong(start + 4, holder.getLong(HALF_OF_YEAR));
			}
			else if (objClass == QuarterPeriodHolder.class)
			{
				row.setInt(start + 3, 2);
				row.setLong(start + 4, holder.getLong(QUARTER_OF_YEAR));
			}
			else if (objClass == MonthPeriodHolder.class)
			{
				row.setInt(start + 3, 3);
				row.setLong(start + 4, holder.getLong(MONTH_OF_YEAR));
			}
		}
		
		if (type == 2)
			row.setLong(start + 5, obj.getEndDate().get().toEpochDay());
		else if (type == 3)
		{
			PeriodHolder<?> holder = (PeriodHolder<?>) ((GenericTimeValue<?>) obj).get().getEndTime().get();
			Class<?> objClass = holder.getClass();
			
			row.setInt(start + 6, holder.get(YEAR));
			if (objClass == YearPeriodHolder.class)
				row.setInt(start + 7, 0);
			else if (objClass == SemesterPeriodHolder.class)
			{
				row.setInt(start + 7, 1);
				row.setLong(start + 8, holder.getLong(HALF_OF_YEAR));
			}
			else if (objClass == QuarterPeriodHolder.class)
			{
				row.setInt(start + 7, 2);
				row.setLong(start + 8, holder.getLong(QUARTER_OF_YEAR));
			}
			else if (objClass == MonthPeriodHolder.class)
			{
				row.setInt(start + 7, 3);
				row.setLong(start + 8, holder.getLong(MONTH_OF_YEAR));
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Class<TimeValue<?, ?, ?, ?>> userClass()
	{
		return (Class<TimeValue<?, ?, ?, ?>>) (Class<? extends TimeValue<?, ?, ?, ?>>) (Class<? extends TimeValue>) TimeValue.class;
	}
}
