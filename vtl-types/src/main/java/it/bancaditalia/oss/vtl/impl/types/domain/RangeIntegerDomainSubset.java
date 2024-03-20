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
package it.bancaditalia.oss.vtl.impl.types.domain;

import java.util.OptionalLong;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class RangeIntegerDomainSubset<S extends IntegerDomainSubset<S>> extends CriterionDomainSubset<RangeIntegerDomainSubset<S>, IntegerValue<?, S>, S, IntegerDomain> implements IntegerDomainSubset<RangeIntegerDomainSubset<S>>
{
	private static final long serialVersionUID = 1L;

	private final OptionalLong minInclusive;
	private final OptionalLong maxInclusive;
	
 	public RangeIntegerDomainSubset(String name, S parent, OptionalLong minInclusive, OptionalLong maxInclusive)
	{
 		super(parent.getName() + (minInclusive.isPresent() ? ">=" + minInclusive.getAsLong() : "") + (maxInclusive.isPresent() ? "<" + maxInclusive.getAsLong() : ""), parent);

 		this.minInclusive = minInclusive;
		this.maxInclusive = maxInclusive;
	}

	@Override
	public Transformation getCriterion()
	{
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean test(IntegerValue<?, S> value)
	{
		Long val = value.get();
		
		return (minInclusive.isEmpty() || minInclusive.getAsLong() < val) && (maxInclusive.isEmpty() || maxInclusive.getAsLong() > val);
	}
	
	@Override
	protected ScalarValue<?, ?, RangeIntegerDomainSubset<S>, IntegerDomain> castCasted(IntegerValue<?, S> value)
	{
		if (test(value))
			return IntegerValue.of(value.get(), this);
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public Variable<RangeIntegerDomainSubset<S>, IntegerDomain> getDefaultVariable()
	{
		return new DefaultVariable<>(this);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((maxInclusive == null) ? 0 : maxInclusive.hashCode());
		result = prime * result + ((minInclusive == null) ? 0 : minInclusive.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof RangeIntegerDomainSubset))
			return false;
		RangeIntegerDomainSubset<?> other = (RangeIntegerDomainSubset<?>) obj;
		if (!maxInclusive.equals(other.maxInclusive))
			return false;
		if (!minInclusive.equals(other.minInclusive))
			return false;
		return true;
	}
}
