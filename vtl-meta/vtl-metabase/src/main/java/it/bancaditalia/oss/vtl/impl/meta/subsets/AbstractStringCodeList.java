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

import java.io.Serializable;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.meta.subsets.AbstractStringCodeList.StringCodeItemImpl;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.StringCodeItem;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public abstract class AbstractStringCodeList implements StringEnumeratedDomainSubset<AbstractStringCodeList, StringCodeItemImpl, String>, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStringCodeList.class);

	public class StringCodeItemImpl extends StringValue<StringCodeItemImpl, AbstractStringCodeList> implements StringCodeItem<StringCodeItemImpl, String, AbstractStringCodeList>
	{
		private static final long serialVersionUID = 1L;

		public StringCodeItemImpl(String value)
		{
			super(value, AbstractStringCodeList.this);
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

	private final StringDomainSubset<?> parent;
	private final String name; 

	private int hashCode;

	public AbstractStringCodeList(StringDomainSubset<?> parent, String name)
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
		StringEnumeratedDomainSubset<?, ?, ?> other = (StringEnumeratedDomainSubset<?, ?, ?>) obj;
		if (!name.equals(other.getName()))
			return false;

		return getCodeItems().equals(other.getCodeItems());
	}

	@Override
	public StringCodeItemImpl cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof StringValue)
		{
			StringCodeItemImpl item = new StringCodeItemImpl((String) value.get());
			if (getCodeItems().contains(item))
				return item;

			LOGGER.warn("Code {} was not found on codelist {}:{}", value.get(), name, new TreeSet<>(getCodeItems()));
		}

		throw new VTLCastException(this, value);
	}

	@Override
	public ScalarValue<?, ?, AbstractStringCodeList, StringDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
	
	protected void setHashCode(int hashCode)
	{
		this.hashCode = hashCode;
	}
	
	@Override
	public String toString()
	{
		return name + ":" + parent;
	}
}
