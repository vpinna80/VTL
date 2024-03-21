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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.OptionalInt;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class StrlenDomainSubset<S extends StringDomainSubset<S>> extends CriterionDomainSubset<StrlenDomainSubset<S>, StringValue<?, S>, S, StringDomain> implements StringDomainSubset<StrlenDomainSubset<S>>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final OptionalInt minLenInclusive;
	private final OptionalInt maxLenInclusive;
	
	public StrlenDomainSubset(S parent, OptionalInt minLenInclusive, OptionalInt maxLenInclusive)
	{
		super(parent.getName() + (minLenInclusive.isPresent() ? ">=" + minLenInclusive.getAsInt() : "") + (maxLenInclusive.isPresent() ? "<=" + maxLenInclusive.getAsInt() : ""), parent);
		
		this.minLenInclusive = requireNonNull(minLenInclusive);
		this.maxLenInclusive = requireNonNull(maxLenInclusive);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return getParentDomain().isAssignableFrom(other);
	}

	@Override
	protected ScalarValue<?, ?, StrlenDomainSubset<S>, StringDomain> castCasted(StringValue<?, S> casted)
	{
		if (test(casted))
			return new StringValue<>(casted.get(), this);
		else
			throw new VTLCastException(this, casted); 
	}

	@Override
	public Transformation getCriterion()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean test(StringValue<?, S> value)
	{
		int len = ((String) value.get()).length();
		if (minLenInclusive.isPresent() && len < minLenInclusive.getAsInt())
			return false;
		if (maxLenInclusive.isPresent() && len > maxLenInclusive.getAsInt())
			return false;
		return true;
	}

	@Override
	public Variable<StrlenDomainSubset<S>, StringDomain> getDefaultVariable()
	{
		return new DefaultVariable<>(this);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + maxLenInclusive.hashCode();
		result = prime * result + minLenInclusive.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof StrlenDomainSubset))
			return false;
		StrlenDomainSubset<?> other = (StrlenDomainSubset<?>) obj;
		if (!maxLenInclusive.equals(other.maxLenInclusive))
			return false;
		if (!minLenInclusive.equals(other.minLenInclusive))
			return false;
		return true;
	}
}
