package it.bancaditalia.oss.vtl.impl.environment.spark;

import java.math.BigDecimal;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;

public class BigDecimalValueUDT extends UserDefinedType<BigDecimalValue<?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public BigDecimalValue<?> deserialize(Object datum)
	{
		return (BigDecimalValue<?>) BigDecimalValue.of((BigDecimal) datum);
	}

	@Override
	public Object serialize(BigDecimalValue<?> obj)
	{
		return obj.get();
	}

	@Override
	public DataType sqlType()
	{
		return DecimalType.USER_DEFAULT();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<BigDecimalValue<?>> userClass()
	{
		return (Class<BigDecimalValue<?>>) (Class<? extends BigDecimalValue<?>>) BigDecimalValue.class;
	}
	
	@Override
	public String toString()
	{
		return "BigDecimalValue";
	}
}
