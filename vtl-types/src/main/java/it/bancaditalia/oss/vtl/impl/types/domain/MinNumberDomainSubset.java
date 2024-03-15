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

public class MinNumberDomainSubset implements NumberDomainSubset<MinNumberDomainSubset, NumberDomain>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final NullValue<MinNumberDomainSubset, NumberDomain> NULL_INSTANCE = NullValue.instance(this);
	private final String name;
	private final NumberDomainSubset<?, ?> parent;
	private final double min;
	
	public MinNumberDomainSubset(String name, NumberDomainSubset<?, ?> parent, double min)
	{
		this.name = requireNonNull(name);
		this.parent = requireNonNull(parent);
		this.min = min;
	}

	@Override
	public NumberDomainSubset<?, ?> getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, MinNumberDomainSubset, NumberDomain> getDefaultValue()
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
	public ScalarValue<?, ?, MinNumberDomainSubset, NumberDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		value = parent.cast(value);
		
		if (value instanceof NullValue)
			return NULL_INSTANCE;
		
		Double num = (Double) value.get();
		if (num >= min)
			return DoubleValue.of(num, this);
		else
			throw new VTLCastException(this, value); 
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) min;
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
		MinNumberDomainSubset other = (MinNumberDomainSubset) obj;
		if (min != other.min)
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
