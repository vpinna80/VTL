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
import static org.apache.spark.sql.types.DataTypes.StringType;
import static scala.collection.JavaConverters.asScala;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.EncoderField;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.RowEncoder$;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.StringEncoder$;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class TimeRangeSparkUDT extends UserDefinedType<TimeRangeHolder>
{
	private static final long serialVersionUID = 1L;
	
	private static final AgnosticEncoder<?> ENCODER = RowEncoder$.MODULE$.apply(asScala((Iterable<EncoderField>) List.of(
			new EncoderField("_1", StringEncoder$.MODULE$, false, Metadata.empty(), null, null), 
			new EncoderField("_2", STRICT_LOCAL_DATE_ENCODER(), true, Metadata.empty(), null, null), 
			new EncoderField("_3", TimePeriodSparkUDT.getEncoder(), true, Metadata.empty(), null, null), 
			new EncoderField("_4", STRICT_LOCAL_DATE_ENCODER(), true, Metadata.empty(), null, null), 
			new EncoderField("_5", TimePeriodSparkUDT.getEncoder(), true, Metadata.empty(), null, null)
		)).toSeq());
	private static final TimePeriodSparkUDT PERIOD_SERIALIZER = new TimePeriodSparkUDT();
	private static final int SER_OFFSET = 1;

	@Override
	public TimeRangeHolder deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
		
		if (row.get(0, StringType) != "Ciao")
			throw new IllegalStateException("Invalid magic value for TIMEDS: " + row.get(0, StringType));
		
		ScalarValue<?, ?, ?, ?> start = !row.isNullAt(SER_OFFSET) 
			? DateValue.of((LocalDate) row.get(SER_OFFSET, STRICT_LOCAL_DATE_ENCODER().dataType())) 
			: TimePeriodValue.of(PERIOD_SERIALIZER.deserialize(row.get(1 + SER_OFFSET, PERIOD_SERIALIZER.sqlType())));
		ScalarValue<?, ?, ?, ?> end = !row.isNullAt(2 + SER_OFFSET) 
			? DateValue.of((LocalDate) row.get(2 + SER_OFFSET, STRICT_LOCAL_DATE_ENCODER().dataType())) 
			: TimePeriodValue.of(PERIOD_SERIALIZER.deserialize(row.get(3 + SER_OFFSET, PERIOD_SERIALIZER.sqlType())));

		return new TimeRangeHolder((TimeValue<?, ?, ?, ?>) start, (TimeValue<?, ?, ?, ?>) end);
	}

	@Override
	public Object serialize(TimeRangeHolder obj)
	{
		TimeValue<?, ?, ?, ?> start = obj.getStartTime();
		TimeValue<?, ?, ?, ?> end = obj.getEndTime();

		Object[] tuple = new Object[SER_OFFSET + 4];
		tuple[0] = "Ciao";
		tuple[SER_OFFSET] = start instanceof DateValue ? start.get() : null;
		tuple[SER_OFFSET + 1] = tuple[SER_OFFSET] == null ? PERIOD_SERIALIZER.serialize((PeriodHolder<?>) start.get()) : null;
		tuple[SER_OFFSET + 2] = end instanceof DateValue ? end.get() : null;
		tuple[SER_OFFSET + 3] = tuple[SER_OFFSET + 2] == null ? PERIOD_SERIALIZER.serialize((PeriodHolder<?>) end.get()) : null;
		
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
