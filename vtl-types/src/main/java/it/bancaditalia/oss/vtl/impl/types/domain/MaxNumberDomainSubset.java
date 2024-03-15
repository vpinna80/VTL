package it.bancaditalia.oss.vtl.impl.types.domain;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class MaxNumberDomainSubset implements NumberDomainSubset<MaxNumberDomainSubset, NumberDomain>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final NullValue<MaxNumberDomainSubset, NumberDomain> NULL_INSTANCE = NullValue.instance(this);
	private final String name;
	private final NumberDomainSubset<?, ?> parent;
	private final double max;
	
	public MaxNumberDomainSubset(String name, NumberDomainSubset<?, ?> parent, double max)
	{
		this.name = requireNonNull(name);
		this.parent = requireNonNull(parent);
		this.max = max;
	}

	@Override
	public NumberDomainSubset<?, ?> getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, MaxNumberDomainSubset, NumberDomain> getDefaultValue()
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
	public ScalarValue<?, ?, MaxNumberDomainSubset, NumberDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		value = parent.cast(value);
		
		if (value instanceof NullValue)
			return NULL_INSTANCE;
		
		Double num = (Double) value.get();
		if (num < max)
			return DoubleValue.of(num, this);
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
		MaxNumberDomainSubset other = (MaxNumberDomainSubset) obj;
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
