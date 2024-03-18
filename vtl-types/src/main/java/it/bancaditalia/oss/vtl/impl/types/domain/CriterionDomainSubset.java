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

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public abstract class CriterionDomainSubset<C extends CriterionDomainSubset<C, V, S, D>, V extends ScalarValue<?, ?, S, D>, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements DescribedDomainSubset<C, V, S, D>
{
	private static final long serialVersionUID = 1L;

	private final String name;
	private final S parent;
	
	@SuppressWarnings("unchecked")
	private transient final ScalarValue<?, ?, C, D> INSTANCE = NullValue.instance((C) this);
	
 	public CriterionDomainSubset(String name, S parent)
	{
		this.name = name;
		this.parent = parent;
	}

	@SuppressWarnings("unchecked")
	@Override
	public D getParentDomain()
	{
		return (D) parent;
	}

	@Override
	public final ScalarValue<?, ?, C, D> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof NullValue)
			return INSTANCE;
		
		@SuppressWarnings("unchecked")
		V casted = (V) parent.cast(value);
		
		return castCasted((V) casted);
	}

	protected abstract ScalarValue<?, ?, C, D> castCasted(V casted);

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return parent.isAssignableFrom(other);
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return parent.isComparableWith(other);  
	}
	
	@Override
	public ScalarValue<?, ?, C, D> getDefaultValue()
	{
		return INSTANCE;
	}
	
	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public String toString()
	{
		return name;
	}
}
