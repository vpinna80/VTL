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
import it.bancaditalia.oss.vtl.impl.types.domain.DefaultVariable;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class IntegerCodeList implements EnumeratedDomainSubset<IntegerCodeList, IntegerDomain>, IntegerDomainSubset<IntegerCodeList>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final VTLAlias alias; 
	private final IntegerDomainSubset<?> parent;
	private final Set<CodeItem<?, ?, IntegerCodeList, IntegerDomain>> items = new HashSet<>();
	private final int hashCode;

	public class IntegerCodeItem extends IntegerValue<IntegerCodeItem, IntegerCodeList> implements CodeItem<IntegerCodeItem, Long, IntegerCodeList, IntegerDomain>
	{
		private static final long serialVersionUID = 1L;
		
		public IntegerCodeItem(Long value, IntegerCodeList parent)
		{
			super(value, parent);
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
	
	public IntegerCodeList(VTLAlias name, IntegerDomainSubset<?> parent, Set<? extends Long> items)
	{
		this.alias = name;
		this.parent = parent;
		this.hashCode = 31 + name.hashCode();
		for (Long item: items)
			this.items.add(new IntegerCodeItem(item, this));
	}
	
	@Override
	public VTLAlias getAlias()
	{
		return alias;
	}

	@Override
	public IntegerDomainSubset<?> getParentDomain()
	{
		return parent;
	}

	@Override
	public IntegerCodeItem cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof IntegerValue)
		{
			IntegerCodeItem item = new IntegerCodeItem((Long) value.get(), this);
			if (items.contains(item))
				return item;
		}

		throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof IntegerCodeList && alias.equals(((IntegerCodeList) other).alias);
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return INTEGERDS.isComparableWith(other);
	}

	@Override
	public Set<CodeItem<?, ?, IntegerCodeList, IntegerDomain>> getCodeItems()
	{
		return items;
	}
	
	@Override
	public String toString()
	{
		return alias + ":" + parent;
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
		IntegerCodeList other = (IntegerCodeList) obj;
		if (!alias.equals(other.alias))
			return false;
		if (!items.equals(other.items))
			return false;

		return true;
	}

	@Override
	public Variable<IntegerCodeList, IntegerDomain> getDefaultVariable()
	{
		return new DefaultVariable<>(this);
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return INTEGERDS.getRepresentation();
	}

	@Override
	public Class<?> getValueClass()
	{
		return IntegerCodeItem.class;
	}
}
