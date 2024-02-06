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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList.StringCodeItem;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class StringCodeList implements EnumeratedDomainSubset<StringCodeList, StringDomain, StringCodeItem, String>, StringDomainSubset<StringCodeList>, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(StringCodeList.class);

	public static class StringCodeItem extends StringValue<StringCodeItem, StringCodeList> implements CodeItem<StringCodeItem, String, StringCodeList, StringDomain>
	{
		private static final long serialVersionUID = 1L;

		public StringCodeItem(String value, StringCodeList cl)
		{
			super(value, cl);
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

	protected final StringDomainSubset<?> parent;
	protected final String name; 
	protected final Set<StringCodeItem> items = new HashSet<>();
	
	protected int hashCode;

	public StringCodeList(StringDomainSubset<?> parent, String name, Set<? extends String> items)
	{
		this.name = name;
		this.parent = parent;
		for (String item: items)
			this.items.add(new StringCodeItem(item, this));

		hashCode = 31 + name.hashCode() + this.items.hashCode();
	}
	

	public StringCodeList(StringDomainSubset<?> parent, String name)
	{
		this.name = name;
		this.parent = parent;
	}


	@Override
	public final String getName()
	{
		return name;
	}

	@Override
	public final StringDomainSubset<?> getParentDomain()
	{
		return parent;
	}

	@Override
	public final boolean isComparableWith(ValueDomain other)
	{
		return STRINGDS.isComparableWith(other);
	}

	public final int hashCode()
	{
		return hashCode;
	}

	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof StringEnumeratedDomainSubset))
			return false;
		StringEnumeratedDomainSubset<?, ?> other = (StringEnumeratedDomainSubset<?, ?>) obj;
		if (!name.equals(other.getName()))
			return false;

		return getCodeItems().equals(other.getCodeItems());
	}
	
	public StringCodeItem getCodeItem(String literal)
	{
		final ScalarValue<?, ?, EntireStringDomainSubset, StringDomain> value = StringValue.of(literal);
		if (getCodeItems().contains(value))
			return new StringCodeItem(literal, this);
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public ScalarValue<?, ?, StringCodeList, StringDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof NullValue)
			return NullValue.instance(this);
		else if (value instanceof StringValue)
		{
			if (getCodeItems().contains(value))
				return new StringCodeItem((String) value.get(), this);

			LOGGER.warn("Code {} was not found on codelist {} = {}", value.get(), name, new TreeSet<>(getCodeItems()));
		}

		throw new VTLCastException(this, value);
	}

	@Override
	public ScalarValue<?, ?, StringCodeList, StringDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
	
	@Override
	public String toString()
	{
		return name + ":" + parent;
	}

	@Override
	public Set<StringCodeItem> getCodeItems()
	{
		return items;
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof StringCodeList && name.equals(((StringCodeList) other).name);
	}
}
