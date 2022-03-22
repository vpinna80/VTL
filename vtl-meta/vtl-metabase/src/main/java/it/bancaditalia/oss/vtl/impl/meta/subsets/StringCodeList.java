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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.UnaryOperator;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.meta.subsets.StringCodeList.StringCodeItemImpl;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.StringCodeItem;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class StringCodeList<I extends StringDomainSubset<I>> implements StringEnumeratedDomainSubset<StringCodeList<I>, I, StringCodeItemImpl<I>, String>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final String name; 
	private final Set<StringCodeItemImpl<I>> items = new HashSet<>();
	private final int hashCode;
	private final StringDomainSubset<I> parent;

	public static class StringCodeItemImpl<I extends StringDomainSubset<I>> extends StringValue<StringCodeItemImpl<I>, StringCodeList<I>> implements StringCodeItem<StringCodeItemImpl<I>, String, StringCodeList<I>, I>
	{
		private static final long serialVersionUID = 1L;

		public StringCodeItemImpl(String value, StringCodeList<I> codeList)
		{
			super(value, codeList);
		}

		@Override
		public int compareTo(ScalarValue<?, ?, ?, ?> o)
		{
			return get().compareTo((String) STRINGDS.cast(o).get());
		}

		@Override
		public String toString()
		{
			return '"' + get() + '"';
		}
	}
	
	public StringCodeList(StringDomainSubset<I> parent, String name, Set<? extends String> items)
	{
		this.parent = parent;
		this.name = name;
		this.hashCode = 31 + name.hashCode();
		for (String item: items)
			this.items.add(new StringCodeItemImpl<>(item, this));
	}
	
	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public StringCodeList<I> getDomain()
	{
		return this;
	}

	@Override
	public StringDomainSubset<I> getParentDomain()
	{
		return parent;
	}

	@Override
	public StringCodeItemImpl<I> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof StringValue)
		{
			StringCodeItemImpl<I> item = new StringCodeItemImpl<>((String) value.get(), this);
			if (items.contains(item))
				return item;
		}

		throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof StringEnumeratedDomainSubset && getVarName().equals(other.getVarName());
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return STRINGDS.isComparableWith(other);
	}

	@Override
	public String getVarName()
	{
		return name + "_var";
	}

	@Override
	public Set<StringCodeItemImpl<I>> getCodeItems()
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
		StringCodeList<?> other = (StringCodeList<?>) obj;
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
	public StringCodeList<I> trim()
	{
		return stringCodeListHelper("TRIM("+name+")", String::trim);
	}

	@Override
	public StringCodeList<I> ltrim()
	{
		return stringCodeListHelper("LTRIM("+name+")", s -> s.replaceAll("^\\s+",""));
	}

	@Override
	public StringCodeList<I> rtrim()
	{
		return stringCodeListHelper("RTRIM("+name+")", s -> s.replaceAll("\\s+$",""));
	}

	@Override
	public StringCodeList<I> ucase()
	{
		return stringCodeListHelper("UCASE("+name+")", String::toUpperCase);
	}

	@Override
	public StringCodeList<I> lcase()
	{
		return stringCodeListHelper("LCASE("+name+")", String::toLowerCase);
	}

	private StringCodeList<I> stringCodeListHelper(String newName, UnaryOperator<String> mapper)
	{
		return new StringCodeList<>(parent, newName, items.stream().map(StringCodeItem::get).map(mapper).collect(toSet()));
	}
	
	@Override
	public ScalarValue<?, ?, StringCodeList<I>, StringDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
}
