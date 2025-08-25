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
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class RangeIntegerDomainSubset<S extends RangeIntegerDomainSubset<S>> extends CriterionDomainSubset<RangeIntegerDomainSubset<S>, IntegerDomain> implements IntegerDomainSubset<RangeIntegerDomainSubset<S>>
{
	private static final long serialVersionUID = 1L;

	private final OptionalLong min;
	private final OptionalLong max;
	private final boolean inclusive;
	
 	public RangeIntegerDomainSubset(VTLAlias name, IntegerDomainSubset<?> parent, OptionalLong min, OptionalLong max, boolean inclusive)
	{
 		super(VTLAliasImpl.of(parent.getAlias().getName() + (min.isPresent() ? ">=" + min.getAsLong() : "") + (max.isPresent() ? (inclusive ? "<=" : "<") + max.getAsLong() : "")), parent);

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
	public boolean test(ScalarValue<?, ?, ?, IntegerDomain> value)
	{
		Long val = (Long) value.get();
		
		return (min.isEmpty() || min.getAsLong() < val) && (max.isEmpty() || (inclusive ? max.getAsLong() > val : max.getAsLong() >= val));
	}
	
	@Override
	protected ScalarValue<?, ?, RangeIntegerDomainSubset<S>, IntegerDomain> castCasted(ScalarValue<?, ?, RangeIntegerDomainSubset<S>, IntegerDomain> value)
	{
		if (test(value))
			return IntegerValue.of((Long) value.get(), this);
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((max == null) ? 0 : max.hashCode());
		result = prime * result + ((min == null) ? 0 : min.hashCode());
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
		if (!max.equals(other.max))
			return false;
		if (!min.equals(other.min))
			return false;
		return true;
	}

	@Override
	public Class<?> getValueClass()
	{
		return super.getValueClass();
	}
}
