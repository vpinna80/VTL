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
import java.time.Year;
import java.time.YearMonth;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.types.StructType;
import org.threeten.extra.YearHalf;
import org.threeten.extra.YearQuarter;

import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;

public class TimePeriodValueUDT extends ScalarValueUDT<TimePeriodValue<?>>
{
	private static final long serialVersionUID = 1L;
	private static final StructType SQL_TYPE = createStructType(List.of(
			createStructField("year", IntegerType, false),
			createStructField("freq", IntegerType, false),
			createStructField("subyear", LongType, true)
		));
	
	public TimePeriodValueUDT()
	{
		super(SQL_TYPE);
	}
	
	@Override
	public TimePeriodValue<?> deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
		int year = row.getInt(0);
		int tag = row.getInt(1); 
		
		PeriodHolder<?> holder;
		
		switch (tag)
		{
			case 0: holder = new YearPeriodHolder(Year.of(year)); break;
			case 1: holder = new SemesterPeriodHolder(YearHalf.of(year, (int) row.getLong(2))); break;
			case 2: holder = new QuarterPeriodHolder(YearQuarter.of(year, (int) row.getLong(2))); break;
			case 3: holder = new MonthPeriodHolder(YearMonth.of(year, (int) row.getLong(2))); break;
			default: throw new InvalidParameterException("deserialize - bad period");
		}; 
		
		return TimePeriodValue.of(holder);
	}

	@Override
	public InternalRow serialize(TimePeriodValue<?> value)
	{
		SpecificInternalRow row = new SpecificInternalRow(SQL_TYPE);
		
		PeriodHolder<?> holder = value.get();
		Class<?> objClass = holder.getClass();
		
		row.setInt(0, holder.get(YEAR));
		if (objClass == YearPeriodHolder.class)
			row.setInt(1, 0);
		else if (objClass == SemesterPeriodHolder.class)
		{
			row.setInt(1, 1);
			row.setLong(2, holder.getLong(HALF_OF_YEAR));
		}
		else if (objClass == QuarterPeriodHolder.class)
		{
			row.setInt(1, 2);
			row.setLong(2, holder.getLong(QUARTER_OF_YEAR));
		}
		else if (objClass == MonthPeriodHolder.class)
		{
			row.setInt(1, 3);
			row.setLong(2, holder.getLong(MONTH_OF_YEAR));
		}
		else
			throw new UnsupportedOperationException("Spark serialization not implemented for " + objClass);
		
		return row;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<TimePeriodValue<?>> userClass()
	{
		return (Class<TimePeriodValue<?>>) (Class<?>) TimePeriodValue.class;
	}
}
