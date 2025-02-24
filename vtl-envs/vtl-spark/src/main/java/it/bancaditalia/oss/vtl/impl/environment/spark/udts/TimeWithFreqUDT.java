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
package it.bancaditalia.oss.vtl.impl.environment.spark.udts;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.DateValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.GenericTimeValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.TimePeriodValueUDT;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeWithFreq;

public class TimeWithFreqUDT extends UserDefinedType<TimeWithFreq>
{
	private static final long serialVersionUID = 1L;
	private static final DateValueUDT DV_UDT = new DateValueUDT();
	private static final TimePeriodValueUDT TPV_UDT = new TimePeriodValueUDT();
	private static final GenericTimeValueUDT GTV_UDT = new GenericTimeValueUDT();
	private static final StructType SQL_TYPE = createStructType(List.of(
			createStructField("freq", IntegerType, false),
			createStructField("current1", DV_UDT, true),
			createStructField("current2", TPV_UDT, true),
			createStructField("current3", GTV_UDT, true)
		));

	@Override
	public TimeWithFreq deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
		TimeWithFreq twf = new TimeWithFreq();
		twf.freq = row.getInt(0);
		if (!row.isNullAt(1))
			twf.current = (TimeValue<?, ?, ?, ?>) row.get(1, DV_UDT);
		else if (!row.isNullAt(2))
			twf.current = (TimeValue<?, ?, ?, ?>) row.get(2, TPV_UDT);
		else if (!row.isNullAt(3))
			twf.current = (TimeValue<?, ?, ?, ?>) row.get(3, GTV_UDT);
		else
			throw new IllegalStateException();
		
		return twf;
	}

	@Override
	public Object serialize(TimeWithFreq obj)
	{
		SpecificInternalRow row = new SpecificInternalRow(SQL_TYPE);
		
		row.setInt(0, obj.freq);
		Class<?> clazz = obj.current.getClass();
		if (clazz == DateValue.class)
			row.update(1, DV_UDT.serialize((DateValue<?>) obj.current));
		else if (clazz == TimePeriodValue.class)
			row.update(2, TPV_UDT.serialize((TimePeriodValue<?>) obj.current));
		else if (clazz == GenericTimeValue.class)
			row.update(3, GTV_UDT.serialize((GenericTimeValue<?>) obj.current));
		
		return row;
	}

	@Override
	public DataType sqlType()
	{
		return SQL_TYPE;
	}

	@Override
	public Class<TimeWithFreq> userClass()
	{
		return TimeWithFreq.class;
	}
}
