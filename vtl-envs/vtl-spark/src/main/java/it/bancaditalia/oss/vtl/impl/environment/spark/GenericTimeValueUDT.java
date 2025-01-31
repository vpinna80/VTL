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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER;
import static scala.collection.JavaConverters.asScala;

import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.EncoderField;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.RowEncoder$;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.UTF8String;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;

public class GenericTimeValueUDT extends UserDefinedType<GenericTimeValue<?>>
{
	private static final long serialVersionUID = 1L;
	
	private static final TimePeriodValueUDT TIMEPERIOD_UDT = new TimePeriodValueUDT();
	private static final AgnosticEncoder<?> PERIOD_ENCODER = TimePeriodValueUDT.ENCODER;
	private static final UTF8String MAGIC = UTF8String.fromBytes("Ciao".getBytes(UTF_8));

	public static final AgnosticEncoder<?> ENCODER = RowEncoder$.MODULE$.apply(asScala((Iterable<EncoderField>) List.of(
			new EncoderField("_1", STRICT_LOCAL_DATE_ENCODER(), true, Metadata.empty(), null, null), 
			new EncoderField("_2", PERIOD_ENCODER, true, Metadata.empty(), null, null), 
			new EncoderField("_3", STRICT_LOCAL_DATE_ENCODER(), true, Metadata.empty(), null, null), 
			new EncoderField("_4", PERIOD_ENCODER, true, Metadata.empty(), null, null)
		)).toSeq());

	@Override
	public GenericTimeValue<?> deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
				
		ScalarValue<?, ?, ? extends TimeDomainSubset<?, ? extends TimeDomain>, ? extends TimeDomain> start = !row.isNullAt(0) 
			? DateValue.of((LocalDate) row.get(0, STRICT_LOCAL_DATE_ENCODER().dataType())) 
			: TIMEPERIOD_UDT.deserialize(row.get(1, TIMEPERIOD_UDT.sqlType()));
		ScalarValue<?, ?, ? extends TimeDomainSubset<?, ? extends TimeDomain>, ? extends TimeDomain> end = !row.isNullAt(2) 
			? DateValue.of((LocalDate) row.get(2, STRICT_LOCAL_DATE_ENCODER().dataType())) 
			: TIMEPERIOD_UDT.deserialize(row.get(3, TIMEPERIOD_UDT.sqlType()));

		return (GenericTimeValue<?>) GenericTimeValue.of(start, end);
	}

	@Override
	public Object serialize(GenericTimeValue<?> obj)
	{
		TimeRangeHolder range = obj.get();
		TimeValue<?, ?, ?, ?> start = range.getStartTime();
		TimeValue<?, ?, ?, ?> end = range.getEndTime();

		InternalRow row = new GenericInternalRow(4);
		row.update(0, MAGIC);
		if (start instanceof DateValue)
		{
			row.setInt(0, (int) ((LocalDate) start.get()).toEpochDay());
			row.setNullAt(1);
			row.setInt(2, (int) ((LocalDate) end.get()).toEpochDay());
			row.setNullAt(3);
		}
		else
		{
			row.setNullAt(0);
			row.update(1, TIMEPERIOD_UDT.serialize((TimePeriodValue<?>) start));
			row.setNullAt(2);
			row.update(3, TIMEPERIOD_UDT.serialize((TimePeriodValue<?>) end));
		}

		return row;
	}

	@Override
	public DataType sqlType()
	{
		return ENCODER.dataType();
	}

	@Override
	public Class<GenericTimeValue<?>> userClass()
	{
		return (Class<GenericTimeValue<?>>) (Class<? extends GenericTimeValue<?>>) GenericTimeValue.class;
	}
	
	@Override
	public String toString()
	{
		return "GenericTimeValue";
	}
	
	@Override
	public String typeName()
	{
		return toString();
	}
}
