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
package it.bancaditalia.oss.vtl.impl.environment.spark;

import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;
import static org.apache.spark.sql.catalyst.util.ArrayData.toArrayData;
import static org.threeten.extra.TemporalFields.HALF_OF_YEAR;

import java.security.InvalidParameterException;
import java.time.Year;
import java.time.YearMonth;

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;
import org.threeten.extra.YearHalf;
import org.threeten.extra.YearQuarter;

import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;

public class TimePeriodValueUDT extends UserDefinedType<TimePeriodValue<?>>
{
	public static final AgnosticEncoder<?> ENCODER = AgnosticEncoders.ArrayEncoder$.MODULE$.apply(AgnosticEncoders.PrimitiveIntEncoder$.MODULE$, false);
	
	private static final long serialVersionUID = 1L;
	private static final ArrayData NULL_ARRAY = toArrayData(new int[] {});

	@Override
	public TimePeriodValue<?> deserialize(Object datum)
	{
		int[] array = (int[]) ((ArrayData) datum).toIntArray();
		
		PeriodHolder<?> holder;
		
		switch (array.length)
		{
			case 0: return null;
			case 1: holder = new YearPeriodHolder(Year.of(array[0])); break;
			case 3: switch (array[0])
				{
					case 0: holder = new SemesterPeriodHolder(YearHalf.of(array[1], array[2])); break;
					case 1: holder = new QuarterPeriodHolder(YearQuarter.of(array[1], array[2])); break;
					case 2: holder = new MonthPeriodHolder(YearMonth.of(array[1], array[2])); break;
					default: throw new InvalidParameterException("deserialize - bad period");
				}; 
				break;
			default: throw new InvalidParameterException("deserialize - bad length: " + array.length);
		}
		
		return TimePeriodValue.of(holder);
	}

	@Override
	public ArrayData serialize(TimePeriodValue<?> value)
	{
		if (value == null)
			return NULL_ARRAY;
		
		PeriodHolder<?> holder = value.get();
		Class<?> objClass = holder.getClass();
		int[] result;
		if (objClass == YearPeriodHolder.class)
			result = new int[] { holder.get(YEAR)};
		else if (objClass == SemesterPeriodHolder.class)
			result = new int[] { 0, (int) holder.getLong(YEAR), (int) holder.getLong(HALF_OF_YEAR)};
		else if (objClass == QuarterPeriodHolder.class)
			result = new int[] { 1, (int) holder.getLong(YEAR), (int) holder.getLong(QUARTER_OF_YEAR)};
		else if (objClass == MonthPeriodHolder.class)
			result = new int[] { 2, (int) holder.getLong(YEAR), (int) holder.getLong(MONTH_OF_YEAR)};
		else
			throw new UnsupportedOperationException("Spark serialization not implemented for " + objClass);
		
		return toArrayData(result);
	}

	@Override
	public DataType sqlType()
	{
		return ENCODER.dataType();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<TimePeriodValue<?>> userClass()
	{
		return (Class<TimePeriodValue<?>>) (Class<? extends TimePeriodValue<?>>) TimePeriodValue.class;
	}
	
	@Override
	public String toString()
	{
		return "TimePeriodValue";
	}
	
	@Override
	public String typeName()
	{
		return toString();
	}
}
