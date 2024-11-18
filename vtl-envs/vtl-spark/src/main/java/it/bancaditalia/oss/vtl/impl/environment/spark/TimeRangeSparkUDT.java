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

import static org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER;
import static scala.collection.JavaConverters.asScala;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import scala.collection.immutable.Seq;

public class TimeRangeSparkUDT extends UserDefinedType<TimeRangeHolder>
{
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unchecked")
	private static final AgnosticEncoder<?> ENCODER = AgnosticEncoders.ProductEncoder$.MODULE$.tuple((Seq<AgnosticEncoder<?>>) asScala((Iterable<?>) List.of(
			(AgnosticEncoder<?>) STRICT_LOCAL_DATE_ENCODER(), 
			(AgnosticEncoder<?>) TimePeriodSparkUDT.getEncoder(), 
			(AgnosticEncoder<?>) STRICT_LOCAL_DATE_ENCODER(), 
			(AgnosticEncoder<?>) TimePeriodSparkUDT.getEncoder() 
		)).toSeq());
	private static final TimePeriodSparkUDT PERIOD_SERIALIZER = new TimePeriodSparkUDT();

	@Override
	public TimeRangeHolder deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
		
		ScalarValue<?, ?, ?, ?> start = !row.isNullAt(0) 
			? DateValue.of((LocalDate) row.get(0, STRICT_LOCAL_DATE_ENCODER().dataType())) 
			: TimePeriodValue.of(PERIOD_SERIALIZER.deserialize(row.get(1, PERIOD_SERIALIZER.sqlType())));
		ScalarValue<?, ?, ?, ?> end = !row.isNullAt(2) 
			? DateValue.of((LocalDate) row.get(2, STRICT_LOCAL_DATE_ENCODER().dataType())) 
			: TimePeriodValue.of(PERIOD_SERIALIZER.deserialize(row.get(3, PERIOD_SERIALIZER.sqlType())));

		return new TimeRangeHolder((TimeValue<?, ?, ?, ?>) start, (TimeValue<?, ?, ?, ?>) end);
	}

	@Override
	public Object serialize(TimeRangeHolder obj)
	{
		TimeValue<?, ?, ?, ?> start = obj.getStartTime();
		TimeValue<?, ?, ?, ?> end = obj.getEndTime();

		Object[] tuple = new Object[4];
		tuple[0] = start instanceof DateValue ? start.get() : null;
		tuple[1] = tuple[0] == null ? PERIOD_SERIALIZER.serialize((PeriodHolder<?>) start.get()) : null;
		tuple[2] = end instanceof DateValue ? end.get() : null;
		tuple[3] = tuple[2] == null ? PERIOD_SERIALIZER.serialize((PeriodHolder<?>) end.get()) : null;
		
		return new GenericInternalRow(tuple);
	}

	@Override
	public DataType sqlType()
	{
		return ENCODER.dataType();
	}

	@Override
	public Class<TimeRangeHolder> userClass()
	{
		return TimeRangeHolder.class;
	}
	
	public static AgnosticEncoder<?> getEncoder()
	{
		return ENCODER;
	}
}
