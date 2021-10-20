package it.bancaditalia.oss.vtl.impl.meta.subsets;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class NumberDomainRangeSubset implements DescribedDomainSubset<NumberDomainRangeSubset, NumberDomain>, NumberDomainSubset<NumberDomainRangeSubset, NumberDomain>
{
	private static final long serialVersionUID = 1L;

	private final String name;
	private final double minInclusive;
	private final double maxInclusive;
	private final NumberDomainSubset<?, ?> parent;
	
 	public NumberDomainRangeSubset(String name, double minInclusive, double maxInclusive, NumberDomainSubset<?, ?> parent)
	{
		this.name = name;
		this.minInclusive = minInclusive;
		this.maxInclusive = maxInclusive;
		this.parent = parent;
	}

	@Override
	public NumberDomain getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, NumberDomainRangeSubset, NumberDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof NullValue)
			return NullValue.instance(this);
		
		double casted = ((Number) parent.cast(value).get()).doubleValue();
		
		if (minInclusive <= casted && casted <= maxInclusive)
			return DoubleValue.of(casted, this);
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other == this;
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return parent.isComparableWith(other);  
	}

	@Override
	public String getVarName()
	{
		return name + "_var";
	}

	@Override
	public Transformation getCriterion()
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public ScalarValue<?, ?, NumberDomainRangeSubset, NumberDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
}
