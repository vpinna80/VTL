package it.bancaditalia.oss.vtl.impl.types.domain;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class MaxIntegerDomainSubset implements IntegerDomainSubset<MaxIntegerDomainSubset>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final NullValue<MaxIntegerDomainSubset, IntegerDomain> NULL_INSTANCE = NullValue.instance(this);
	private final String name;
	private final IntegerDomainSubset<?> parent;
	private final long max;
	
	public MaxIntegerDomainSubset(String name, IntegerDomainSubset<?> parent, long max)
	{
		this.name = requireNonNull(name);
		this.parent = requireNonNull(parent);
		this.max = max;
	}

	@Override
	public IntegerDomain getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, MaxIntegerDomainSubset, IntegerDomain> getDefaultValue()
	{
		return NULL_INSTANCE;
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return parent.isAssignableFrom(other);
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public ScalarValue<?, ?, MaxIntegerDomainSubset, IntegerDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		value = parent.cast(value);
		
		if (value instanceof NullValue)
			return NULL_INSTANCE;
		
		Long num = (Long) value.get();
		if (num < max)
			return new IntegerValue<>(num, this);
		else
			throw new VTLCastException(this, value); 
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) max;
		result = prime * result + name.hashCode();
		result = prime * result + parent.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MaxIntegerDomainSubset other = (MaxIntegerDomainSubset) obj;
		if (max != other.max)
			return false;
		if (!name.equals(other.name))
			return false;
		if (!parent.equals(other.parent))
			return false;
		return true;
	}
	
	@Override
	public String toString()
	{
		return name;
	}
}
