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

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
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
			createStructField("start1", DV_UDT, true),
			createStructField("start2", DV_UDT, true),
			createStructField("end1", TPV_UDT, true),
			createStructField("end2", TPV_UDT, true)
		));

	public GenericTimeValueUDT()
	{
		super(SQL_TYPE);
	}
	
	@Override
	public GenericTimeValue<?> deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
		TimeValue<?, ?, ?, ?> start, end;
		
		if (!row.isNullAt(0))
		{
			start = requireNonNull(DV_UDT.deserialize(row.get(0, DV_UDT)));
			end = requireNonNull(DV_UDT.deserialize(row.get(2, DV_UDT)));
		}
		else if (!row.isNullAt(1))
		{
			start = requireNonNull(TPV_UDT.deserialize(row.get(1, TPV_UDT)));
			end = requireNonNull(TPV_UDT.deserialize(row.get(3, TPV_UDT)));
		}
		else
			return null;

		return (GenericTimeValue<?>) GenericTimeValue.of(start, end);
	}

	@Override
	public Object serialize(GenericTimeValue<?> obj)
	{
		TimeRangeHolder range = obj.get();
		TimeValue<?, ?, ?, ?> start = range.getStartTime();
		TimeValue<?, ?, ?, ?> end = range.getEndTime();

		InternalRow row = new GenericInternalRow(4);
		if (start instanceof DateValue)
		{
			row.update(0, DV_UDT.serialize((DateValue<?>) start));
			row.setNullAt(1);
			row.update(2, DV_UDT.serialize((DateValue<?>) end));
			row.setNullAt(3);
		}
		else
		{
			row.setNullAt(0);
			row.update(1, TPV_UDT.serialize((TimePeriodValue<?>) start));
			row.setNullAt(2);
			row.update(3, TPV_UDT.serialize((TimePeriodValue<?>) end));
		}

		return row;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<GenericTimeValue<?>> userClass()
	{
		return (Class<GenericTimeValue<?>>) (Class<?>) GenericTimeValue.class;
	}
}
