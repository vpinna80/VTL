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

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class NonNullDomainSubset<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements ValueDomainSubset<S, D>  
{
	private static final long serialVersionUID = 1L;
	
	private final S subsetWithNull;
	
	public NonNullDomainSubset(S subsetWithNull)
	{
		this.subsetWithNull = subsetWithNull;
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return subsetWithNull.isAssignableFrom(other);
	}

	@Override
	public String getName()
	{
		return subsetWithNull.getName() + " not null";
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
		if (value instanceof NullValue)
			throw new VTLCastException(this, value);
		else
			return subsetWithNull.cast(value);
	}

	@Override
	public Variable<S, D> getDefaultVariable()
	{
		return subsetWithNull.getDefaultVariable();
	}
}
