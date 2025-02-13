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

import java.io.Serializable;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class NonNullDomainSubset<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements ValueDomainSubset<S, D>  
{
	private static final long serialVersionUID = 1L;
	
	private final S subsetWithNull;
	private final int hashCode;
	
	public NonNullDomainSubset(S subsetWithNull)
	{
		this.subsetWithNull = subsetWithNull;
		hashCode = 31 + subsetWithNull.hashCode();
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return subsetWithNull.isAssignableFrom(other);
	}

	@Override
	public VTLAlias getAlias()
	{
		return VTLAliasImpl.of(subsetWithNull.getAlias() + " not null");
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return subsetWithNull.isComparableWith(other);
	}

	@Override
	public D getParentDomain()
	{
		return subsetWithNull.getParentDomain(); 
	}

	@Override
	public ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value.isNull())
			throw new VTLCastException(this, value);
		else
			return subsetWithNull.cast(value);
	}

	@Override
	public Variable<S, D> getDefaultVariable()
	{
		return subsetWithNull.getDefaultVariable();
	}
	
	@Override
	public String toString()
	{
		return subsetWithNull + " not null";
	}

	@Override
	public int hashCode()
	{
		return hashCode;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!(obj instanceof NonNullDomainSubset))
			return false;
		NonNullDomainSubset<?, ?> other = (NonNullDomainSubset<?, ?>) obj;
		if (subsetWithNull == null)
		{
			if (other.subsetWithNull != null)
				return false;
		}
		else if (!subsetWithNull.equals(other.subsetWithNull))
			return false;
		return true;
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return subsetWithNull.getRepresentation();
	}

	@Override
	public Class<?> getValueClass()
	{
		return subsetWithNull.getValueClass();
	}
}
