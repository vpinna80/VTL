package it.bancaditalia.oss.vtl.impl.environment.spark;

import static org.apache.spark.sql.types.DataTypes.BooleanType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;

public class BooleanValueUDT extends UserDefinedType<BooleanValue<?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public BooleanValue<?> deserialize(Object datum)
	{
		return (BooleanValue<?>) BooleanValue.of((Boolean) datum);
	}

	@Override
	public Object serialize(BooleanValue<?> obj)
	{
		return obj.get();
	}

	@Override
	public DataType sqlType()
	{
		return BooleanType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<BooleanValue<?>> userClass()
	{
		return (Class<BooleanValue<?>>) (Class<? extends BooleanValue<?>>) BooleanValue.class;
	}
	
	@Override
	public String toString()
	{
		return "BooleanValue";
	}
}
