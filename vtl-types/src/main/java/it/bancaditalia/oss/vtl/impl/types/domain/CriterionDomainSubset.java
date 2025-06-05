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

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public abstract class CriterionDomainSubset<S extends CriterionDomainSubset<S, D>, D extends ValueDomain> implements DescribedDomainSubset<S, D>
{
	private static final long serialVersionUID = 1L;

	private final VTLAlias alias;
	private final D parent;
	
 	public CriterionDomainSubset(VTLAlias alias, D parent)
	{
		this.alias = alias;
		this.parent = parent;
	}

	@Override
	public D getParentDomain()
	{
		return (D) parent;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value.isNull())
			return NullValue.instance((S) this);
		
		return castCasted((ScalarValue<?, ?, S, D>) ((ValueDomainSubset<?, ?>) parent).cast(value));
	}

	protected abstract ScalarValue<?, ?, S, D> castCasted(ScalarValue<?, ?, S, D> casted);

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return parent.isAssignableFrom(other);
	}
	
	@Override
	public VTLAlias getAlias()
	{
		return alias;
	}
	
	@Override
	public String toString()
	{
		return alias.toString();
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((alias == null) ? 0 : alias.hashCode());
		result = prime * result + ((parent == null) ? 0 : parent.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!(obj instanceof CriterionDomainSubset))
			return false;
		CriterionDomainSubset<?, 	?> other = (CriterionDomainSubset<?, ?>) obj;
		if (alias == null)
		{
			if (other.alias != null)
				return false;
		}
		else if (!alias.equals(other.alias))
			return false;
		if (parent == null)
		{
			if (other.parent != null)
				return false;
		}
		else if (!parent.equals(other.parent))
			return false;
		return true;
	}
	
	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return parent.getRepresentation();
	}

	@Override
	public Class<?> getValueClass()
	{
		return parent.getValueClass();
	}
}
