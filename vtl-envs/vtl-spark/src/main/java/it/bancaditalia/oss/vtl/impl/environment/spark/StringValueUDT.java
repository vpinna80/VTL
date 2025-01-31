package it.bancaditalia.oss.vtl.impl.environment.spark;

import static org.apache.spark.sql.types.DataTypes.StringType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.UTF8String;

import it.bancaditalia.oss.vtl.impl.types.data.StringValue;

public class StringValueUDT extends UserDefinedType<StringValue<?, ?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public StringValue<?, ?> deserialize(Object datum)
	{
		return (StringValue<?, ?>) StringValue.of(((UTF8String) datum).toString());
	}

	@Override
	public Object serialize(StringValue<?, ?> obj)
	{
		return UTF8String.fromString(obj.get());
	}

	@Override
	public DataType sqlType()
	{
		return StringType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<StringValue<?, ?>> userClass()
	{
		return (Class<StringValue<?, ?>>) (Class<? extends StringValue<?, ?>>) StringValue.class;
	}
	
	@Override
	public String toString()
	{
		return "StringValue";
	}
}
