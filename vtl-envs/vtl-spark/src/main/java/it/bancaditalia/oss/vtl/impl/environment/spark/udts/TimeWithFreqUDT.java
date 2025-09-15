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

import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.TimeValueUDT;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeWithFreq;

public class TimeWithFreqUDT extends UserDefinedType<TimeWithFreq>
{
	private static final long serialVersionUID = 1L;
	private static final TimeValueUDT TV_UDT = new TimeValueUDT();
	private static final StructType SQL_TYPE = createStructType(List.of(
			createStructField("freq", IntegerType, false),
			createStructField("time", TV_UDT, true)
		));

	@Override
	public TimeWithFreq deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
		TimeWithFreq twf = new TimeWithFreq();
		
		twf.freq = row.getInt(0);
		twf.current = TV_UDT.deserialize(row.get(1, TV_UDT));
		
		return twf;
	}

	@Override
	public Object serialize(TimeWithFreq obj)
	{
		SpecificInternalRow row = new SpecificInternalRow(SQL_TYPE);
		
		row.setInt(0, obj.freq);
		row.update(1, TV_UDT.serialize(obj.current));
		
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
