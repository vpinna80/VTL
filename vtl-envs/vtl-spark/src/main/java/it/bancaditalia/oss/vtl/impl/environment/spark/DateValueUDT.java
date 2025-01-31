package it.bancaditalia.oss.vtl.impl.environment.spark;

import static org.apache.spark.sql.types.DataTypes.DateType;

import java.time.temporal.TemporalAccessor;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;

public class DateValueUDT extends UserDefinedType<DateValue<?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public DateValue<?> deserialize(Object datum)
	{
		return (DateValue<?>) DateValue.of((TemporalAccessor) datum);
	}

	@Override
	public Object serialize(DateValue<?> obj)
	{
		return obj.get();
	}

	@Override
	public DataType sqlType()
	{
		return DateType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<DateValue<?>> userClass()
	{
		return (Class<DateValue<?>>) (Class<? extends DateValue<?>>) DateValue.class;
	}
	
	@Override
	public String toString()
	{
		return "DateValue";
	}
}
