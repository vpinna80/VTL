/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
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
