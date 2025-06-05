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

import java.util.OptionalDouble;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class RangeNumberDomainSubset<S extends RangeNumberDomainSubset<S>> extends CriterionDomainSubset<S, NumberDomain> implements NumberDomainSubset<S, NumberDomain>
{
	private static final long serialVersionUID = 1L;

	private final OptionalDouble min;
	private final OptionalDouble max;
	private final boolean inclusive;
	
 	public RangeNumberDomainSubset(VTLAlias name, NumberDomain parent, OptionalDouble min, OptionalDouble max, boolean inclusive)
	{
 		super(VTLAliasImpl.of(parent.getAlias() + (min.isPresent() ? ">=" + min.getAsDouble() : "") + (max.isPresent() ? (inclusive ? "<=" : "<") + max.getAsDouble() : "")), parent);

 		this.min = min;
		this.max = max;
		this.inclusive = inclusive;
	}

	@Override
	public Transformation getCriterion()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean test(ScalarValue<?, ?, S, NumberDomain> value)
	{
		Double val = (Double) value.get();
		
		return (min.isEmpty() || min.getAsDouble() < val) && (max.isEmpty() || (inclusive ? max.getAsDouble() > val : max.getAsDouble() >= val));
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected ScalarValue<?, ?, S, NumberDomain> castCasted(ScalarValue<?, ?, S, NumberDomain> value)
	{
		if (test(value))
			return DoubleValue.of((Double) value.get(), (S) this);
		else
			throw new VTLCastException(this, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Variable<S, NumberDomain> getDefaultVariable()
	{
		return new DefaultVariable<>((S) this);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + max.hashCode();
		result = prime * result + min.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof RangeNumberDomainSubset))
			return false;
		RangeNumberDomainSubset<?> other = (RangeNumberDomainSubset<?>) obj;
		if (!max.equals(other.max))
			return false;
		if (!min.equals(other.min))
			return false;
		return true;
	}
}
