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

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class NullDomain implements ValueDomainSubset<NullDomain, ValueDomain> 
{
	private static final long serialVersionUID = 1L;
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return false;
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return false;
	}

	@Override
	public ValueDomain getParentDomain()
	{
		return null;
	}
	
	@Override
	public ScalarValue<?, ?, NullDomain, ValueDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		throw new UnsupportedOperationException("Cast to unknown domain not supported.");
	}
	
	@Override
	public int hashCode()
	{
		return getClass().hashCode();
	}
	
	@Override
	public boolean equals(Object obj)
	{
		return obj != null && obj.getClass() == getClass();
	}
	
	@Override
	public String getName()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Variable<NullDomain, ValueDomain> getDefaultVariable()
	{
		throw new UnsupportedOperationException();
	}
}