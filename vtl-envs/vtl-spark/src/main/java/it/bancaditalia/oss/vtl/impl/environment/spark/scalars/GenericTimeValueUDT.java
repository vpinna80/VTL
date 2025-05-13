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

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;

public class GenericTimeValueUDT extends ScalarValueUDT<GenericTimeValue<?>>
{
	private static final long serialVersionUID = 1L;
	private static final DateValueUDT DV_UDT = new DateValueUDT();
	private static final TimePeriodValueUDT TPV_UDT = new TimePeriodValueUDT();
	private static final StructType SQL_TYPE = createStructType(List.of(
			createStructField("startdate", LongType, true),
			createStructField("startyear", IntegerType, true),
			createStructField("startfreq", IntegerType, true),
			createStructField("startsubyear", LongType, true),
			createStructField("enddate", LongType, true),
			createStructField("endyear", IntegerType, true),
			createStructField("endfreq", IntegerType, true),
			createStructField("endsubyear", LongType, true)
		));

	public GenericTimeValueUDT()
	{
		super(SQL_TYPE);
	}
	
	@Override
	protected GenericTimeValue<?> deserializeFrom(InternalRow row, int start)
	{
		TimeValue<?, ?, ?, ?> startTime, endTime;
		
		if (!row.isNullAt(start))
		{
			startTime = DV_UDT.deserializeFrom(row, start);
			endTime = DV_UDT.deserializeFrom(row, start + 4);
		}
		else
		{
			startTime = TPV_UDT.deserializeFrom(row, start + 1);
			endTime = TPV_UDT.deserializeFrom(row, start + 5);
		}

		return (GenericTimeValue<?>) GenericTimeValue.of(startTime, endTime);
	}

	@Override
	protected void serializeTo(GenericTimeValue<?> obj, InternalRow row, int start)
	{
		TimeRangeHolder range = obj.get();
		TimeValue<?, ?, ?, ?> startTime = range.getStartTime();
		TimeValue<?, ?, ?, ?> endTime = range.getEndTime();

		if (startTime instanceof DateValue)
		{
			DV_UDT.serializeTo((DateValue<?>) startTime, row, start);
			row.setNullAt(1);
			row.setNullAt(2);
			row.setNullAt(3);
			DV_UDT.serializeTo((DateValue<?>) endTime, row, start + 4);
			row.setNullAt(5);
			row.setNullAt(6);
			row.setNullAt(7);
		}
		else
		{
			row.setNullAt(0);
			TPV_UDT.serializeTo((TimePeriodValue<?>) endTime, row, start + 1);
			row.setNullAt(4);
			TPV_UDT.serializeTo((TimePeriodValue<?>) endTime, row, start + 5);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<GenericTimeValue<?>> userClass()
	{
		return (Class<GenericTimeValue<?>>) (Class<?>) GenericTimeValue.class;
	}
}
