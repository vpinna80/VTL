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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.OptionalInt;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class StrlenDomainSubset implements StringDomainSubset<StrlenDomainSubset>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final NullValue<StrlenDomainSubset, StringDomain> NULL_INSTANCE = NullValue.instance(this);
	private final String name;
	private final StringDomainSubset<?> parent;
	private final OptionalInt minLen;
	private final OptionalInt maxLen;
	
	public StrlenDomainSubset(String name, StringDomainSubset<?> parent, OptionalInt minLen, OptionalInt maxLen)
	{
		this.name = requireNonNull(name);
		this.parent = requireNonNull(parent);
		this.minLen = requireNonNull(minLen);
		this.maxLen = requireNonNull(maxLen);
	}

	@Override
	public StringDomain getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, StrlenDomainSubset, StringDomain> getDefaultValue()
	{
		return NULL_INSTANCE;
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return parent.isAssignableFrom(other);
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public ScalarValue<?, ?, StrlenDomainSubset, StringDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		value = parent.cast(value);
		
		if (value instanceof NullValue)
			return NULL_INSTANCE;
		
		String str = (String) value.get();
		if ((minLen.isEmpty() || str.length() >= minLen.getAsInt()) && (maxLen.isEmpty() || str.length() < maxLen.getAsInt()))
			return new StringValue<>(str, this);
		else
			throw new VTLCastException(this, value); 
	}
	
	@Override
	public String toString()
	{
		return name;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + maxLen.hashCode();
		result = prime * result + minLen.hashCode();
		result = prime * result + name.hashCode();
		result = prime * result + parent.hashCode();
		return result;
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
		StrlenDomainSubset other = (StrlenDomainSubset) obj;
		if (!maxLen.equals(other.maxLen))
			return false;
		if (!minLen.equals(other.minLen))
			return false;
		if (!name.equals(other.name))
			return false;
		if (!parent.equals(other.parent))
			return false;
		return true;
	}
}
