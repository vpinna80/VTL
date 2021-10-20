package it.bancaditalia.oss.vtl.impl.meta.subsets;

import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class RegexpDomainSubset<S extends RegexpDomainSubset<S, D>, D extends ValueDomain> implements DescribedDomainSubset<S, D>
{
	private static final long serialVersionUID = 1L;

	private final String name;
	private final ValueDomainSubset<?,D> parent;
	private final Pattern regexp;
	
	public RegexpDomainSubset(String name, ValueDomainSubset<?, D> parent, Pattern regexp)
	{
		this.name = name;
		this.parent = parent;
		this.regexp = regexp;
	}

	@SuppressWarnings("unchecked")
	@Override
	public D getParentDomain()
	{
		return (D) parent;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value)
	{
		ScalarValue<?, ?, ?, D> casted = parent.cast(value);
		
		if (casted instanceof NullValue)
			return NullValue.instance((S) this);
		
		if (regexp.matcher(value.toString()).matches())
			return (ScalarValue<?, ?, S, D>) value;
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof RegexpDomainSubset && parent.isAssignableFrom(other) 
				&& regexp.equals(((RegexpDomainSubset<?, ?>) other).regexp);
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
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public ScalarValue<?, ?, S, D> getDefaultValue()
	{
		return NullValue.instance((S) this);
	}
}
