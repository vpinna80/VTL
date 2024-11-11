package it.bancaditalia.oss.vtl.impl.environment.spark;

import static org.apache.spark.sql.types.DataTypes.IntegerType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.Frequency;

public class FrequencySparkUDT extends UserDefinedType<Frequency>
{
	private static final long serialVersionUID = 1L;
	private static final Frequency[] VALUES = Frequency.values();
	
	@Override
	public Frequency deserialize(Object datum)
	{
		return datum != null ? VALUES[(Integer) datum] : null;
	}

	@Override
	public Integer serialize(Frequency frequency)
	{
		return frequency != null ? Integer.valueOf(frequency.ordinal()) : null;
	}

	@Override
	public DataType sqlType()
	{
		return IntegerType;
	}

	@Override
	public Class<Frequency> userClass()
	{
		return Frequency.class;
	}
}
