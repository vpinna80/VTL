package it.bancaditalia.oss.vtl.impl.types.domain;

import it.bancaditalia.oss.vtl.model.data.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class DescribedDomainSubsetImpl<S extends DescribedDomainSubsetImpl<S, D>, D extends ValueDomain> implements DescribedDomainSubset<S, D>
{
	private static final long serialVersionUID = 1L;

	private final String name;
	private final D domain;
	private final Transformation criterion;
	
	public DescribedDomainSubsetImpl(String name, D domain, Transformation criterion)
	{
		this.name = name;
		this.domain = domain;
		this.criterion = criterion;
	}

	@Override
	public Transformation getCriterion()
	{
		return criterion;
	}

	@Override
	public D getParentDomain()
	{
		return domain;
	}

	@Override
	public ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value)
	{
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public String getVarName()
	{
		return name + "_var";
	}
}
