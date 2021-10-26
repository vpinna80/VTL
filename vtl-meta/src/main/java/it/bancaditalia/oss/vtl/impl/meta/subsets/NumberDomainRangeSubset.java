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
