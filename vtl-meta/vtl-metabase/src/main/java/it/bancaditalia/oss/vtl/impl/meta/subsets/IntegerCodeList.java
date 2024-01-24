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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.IntegerCodeItem;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class IntegerCodeList<I extends IntegerDomainSubset<I>> implements IntegerEnumeratedDomainSubset<IntegerCodeList<I>, IntegerCodeList<I>.IntegerCodeItemImpl, Long>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final String name; 
	private final Set<IntegerCodeItemImpl> items = new HashSet<>();
	private final int hashCode;
	private final IntegerDomainSubset<I> parent;

	public class IntegerCodeItemImpl extends IntegerValue<IntegerCodeItemImpl, IntegerCodeList<I>> implements IntegerCodeItem<IntegerCodeItemImpl, Long, IntegerCodeList<I>>
	{
		private static final long serialVersionUID = 1L;

		public IntegerCodeItemImpl(Long value)
		{
			super(value, IntegerCodeList.this);
		}

		@Override
		public int compareTo(ScalarValue<?, ?, ?, ?> o)
		{
			return get().compareTo((Long) INTEGERDS.cast(o).get());
		}

		@Override
		public String toString()
		{
			return get().toString();
		}
	}
	
	public IntegerCodeList(IntegerDomainSubset<I> parent, String name, Set<? extends Long> items)
	{
		this.parent = parent;
		this.name = name;
		this.hashCode = 31 + name.hashCode();
		for (Long item: items)
			this.items.add(new IntegerCodeItemImpl(item));
	}
	
	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public IntegerDomainSubset<I> getParentDomain()
	{
		return parent;
	}

	@Override
	public IntegerCodeItemImpl cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof IntegerValue)
		{
			IntegerCodeItemImpl item = new IntegerCodeItemImpl((Long) value.get());
			if (items.contains(item))
				return item;
		}

		throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof IntegerCodeList && name.equals(((IntegerCodeList<?>) other).name);
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return INTEGERDS.isComparableWith(other);
	}

	@Override
	public Set<IntegerCodeItemImpl> getCodeItems()
	{
		return items;
	}
	
	@Override
	public String toString()
	{
		return name + ":" + parent;
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
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntegerCodeList<?> other = (IntegerCodeList<?>) obj;
		if (name == null)
		{
			if (other.name != null)
				return false;
		}
		else if (!name.equals(other.name))
			return false;
		if (!items.equals(other.items))
			return false;

		return true;
	}

	@Override
	public ScalarValue<?, ?, IntegerCodeList<I>, IntegerDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
}
