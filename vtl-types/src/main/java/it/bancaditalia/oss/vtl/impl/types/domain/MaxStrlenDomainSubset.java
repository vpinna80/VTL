package it.bancaditalia.oss.vtl.impl.types.domain;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class MaxStrlenDomainSubset implements StringDomainSubset<MaxStrlenDomainSubset>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final NullValue<MaxStrlenDomainSubset, StringDomain> NULL_INSTANCE = NullValue.instance(this);
	private final String name;
	private final StringDomainSubset<?> parent;
	private final int maxLen;
	
	public MaxStrlenDomainSubset(String name, StringDomainSubset<?> parent, int maxLen)
	{
		this.name = requireNonNull(name);
		this.parent = requireNonNull(parent);
		this.maxLen = maxLen;
	}

	@Override
	public StringDomain getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, MaxStrlenDomainSubset, StringDomain> getDefaultValue()
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
	public ScalarValue<?, ?, MaxStrlenDomainSubset, StringDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		value = parent.cast(value);
		
		if (value instanceof NullValue)
			return NULL_INSTANCE;
		
		String str = (String) value.get();
		if (str.length() <= maxLen)
			return new StringValue<>(str, this);
		else
			throw new VTLCastException(this, value); 
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + maxLen;
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
		MaxStrlenDomainSubset other = (MaxStrlenDomainSubset) obj;
		if (maxLen != other.maxLen)
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
