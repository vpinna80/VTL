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
import java.security.InvalidParameterException;
import java.util.HashSet;
import java.util.Optional;
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
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class StringCodeList implements StringEnumeratedDomainSubset<StringCodeList, StringCodeItem>, Serializable
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
	protected final VTLAlias alias; 
	protected final Set<StringCodeItem> items = new HashSet<>();
	
	protected int hashCode;

	public StringCodeList(StringDomainSubset<?> parent, VTLAlias alias, Set<? extends String> items)
	{
		this.alias = alias;
		this.parent = parent;
		
		for (String item: items)
			this.items.add(new StringCodeItem(item, this));
		hashCode = 31 + alias.hashCode() + this.items.hashCode();

		if (parent instanceof StringEnumeratedDomainSubset)
		{
			Set<? extends CodeItem<?, String, ?, StringDomain>> parentCodeItems = ((StringEnumeratedDomainSubset<?, ?>) parent).getCodeItems();
			for (StringCodeItem item: this.items)
				if (!parentCodeItems.contains(item))
					throw new InvalidParameterException("In codelist " + alias + ", code '" + item 
						+ "' does not belong to the parent domain " + parent.getAlias());
		}
	}

	public StringCodeList(StringDomainSubset<?> parent, VTLAlias alias)
	{
		this.alias = alias;
		this.parent = parent;
	}


	@Override
	public VTLAlias getAlias()
	{
		return alias;
	}

	@Override
	public final StringDomainSubset<?> getParentDomain()
	{
		return parent;
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
		if (!alias.equals(other.getAlias()))
			return false;

		return getCodeItems().equals(other.getCodeItems());
	}
	
	public Optional<StringCodeItem> getCodeItem(String scalarValue)
	{
		final ScalarValue<?, ?, EntireStringDomainSubset, StringDomain> value = StringValue.of(scalarValue);
		if (getCodeItems().contains(value))
			return Optional.of(new StringCodeItem(scalarValue, this));
		else
			return Optional.empty();
	}

	@Override
	public ScalarValue<?, ?, StringCodeList, StringDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value.isNull())
			return NullValue.instance(this);
		else if (value instanceof StringValue)
		{
			if (getCodeItems().contains(value))
				return new StringCodeItem((String) value.get(), this);

			LOGGER.warn("Code {} was not found on codelist {} = {}", value.get(), alias, new TreeSet<>(getCodeItems()));
		}

		throw new VTLCastException(this, value);
	}
	
	@Override
	public String toString()
	{
		return alias + ":" + parent;
	}

	@Override
	public Set<StringCodeItem> getCodeItems()
	{
		return items;
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof StringCodeList && alias.equals(((StringCodeList) other).alias);
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return String.class;
	}

	@Override
	public Class<?> getValueClass()
	{
		return StringCodeItem.class;
	}
}
