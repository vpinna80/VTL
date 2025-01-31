package it.bancaditalia.oss.vtl.impl.environment.spark;

import static org.apache.spark.sql.types.DataTypes.DoubleType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;

public class DoubleValueUDT extends UserDefinedType<DoubleValue<?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public DoubleValue<?> deserialize(Object datum)
	{
		return (DoubleValue<?>) DoubleValue.of((Double) datum, Domains.NUMBERDS);
	}

	@Override
	public Object serialize(DoubleValue<?> obj)
	{
		return obj.get();
	}

	@Override
	public DataType sqlType()
	{
		return DoubleType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<DoubleValue<?>> userClass()
	{
		return (Class<DoubleValue<?>>) (Class<? extends DoubleValue<?>>) DoubleValue.class;
	}
	
	@Override
	public String toString()
	{
		return "DoubleValue";
	}
}
