package it.bancaditalia.oss.vtl.impl.types.domain;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

public class NullDomain implements ValueDomainSubset<NullDomain, ValueDomain> 
{
	private static final long serialVersionUID = 1L;
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return false;
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return false;
	}

	@Override
	public Object getCriterion()
	{
		return null;
	}

	@Override
	public ValueDomain getParentDomain()
	{
		return null;
	}
	
	@Override
	public ScalarValue<?, ?, NullDomain, ValueDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		throw new UnsupportedOperationException("Cast to unknown domain not supported.");
	}

	@Override
	public String getVarName()
	{
		throw new UnsupportedOperationException("No variable name for unknown domain.");
	}
}