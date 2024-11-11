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
