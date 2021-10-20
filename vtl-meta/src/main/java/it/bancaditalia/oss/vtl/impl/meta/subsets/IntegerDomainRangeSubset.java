package it.bancaditalia.oss.vtl.impl.meta.subsets;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class IntegerDomainRangeSubset implements DescribedDomainSubset<IntegerDomainRangeSubset, IntegerDomain>, IntegerDomainSubset<IntegerDomainRangeSubset>
{
	private static final long serialVersionUID = 1L;

	private final String name;
	private final long minInclusive;
	private final long maxInclusive;
	private final IntegerDomainSubset<?> parent;
	
 	public IntegerDomainRangeSubset(String name, long minInclusive, long maxInclusive, IntegerDomainSubset<?> parent)
	{
		this.name = name;
		this.minInclusive = minInclusive;
		this.maxInclusive = maxInclusive;
		this.parent = parent;
	}

	@Override
	public IntegerDomain getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, IntegerDomainRangeSubset, IntegerDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof NullValue)
			return NullValue.instance(this);
		
		long casted = ((Number) parent.cast(value).get()).longValue();
		
		if (minInclusive <= casted && casted <= maxInclusive)
			return IntegerValue.of(casted, this);
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
	public ScalarValue<?, ?, IntegerDomainRangeSubset, IntegerDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
}
