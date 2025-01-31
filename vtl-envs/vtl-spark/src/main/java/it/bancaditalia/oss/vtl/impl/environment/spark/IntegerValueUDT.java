package it.bancaditalia.oss.vtl.impl.environment.spark;

import static org.apache.spark.sql.types.DataTypes.LongType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;

public class IntegerValueUDT extends UserDefinedType<IntegerValue<?, ?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public IntegerValue<?, ?> deserialize(Object datum)
	{
		return (IntegerValue<?, ?>) IntegerValue.of((Long) datum);
	}

	@Override
	public Object serialize(IntegerValue<?, ?> obj)
	{
		return obj.get();
	}

	@Override
	public DataType sqlType()
	{
		return LongType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<IntegerValue<?, ?>> userClass()
	{
		return (Class<IntegerValue<?, ?>>) (Class<? extends IntegerValue<?, ?>>) IntegerValue.class;
	}
	
	@Override
	public String toString()
	{
		return "IntegerValue";
	}
}
