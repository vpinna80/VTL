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
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.util.SerPredicate;

public class RegExpDomainSubset implements StringDomainSubset<RegExpDomainSubset>, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final String name;
	private final String regexpstr;
	private final StringDomainSubset<?> parent;
	private final SerPredicate<String> regexp;
	
	public RegExpDomainSubset(String name, String regexp, StringDomainSubset<?> parent)
	{
		this.name = requireNonNull(name);
		this.regexpstr = requireNonNull(regexp);
		this.parent = requireNonNull(parent);
		this.regexp = Pattern.compile(regexp).asMatchPredicate()::test;
	}

	@Override
	public StringDomain getParentDomain()
	{
		return parent;
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
	public ScalarValue<?, ?, RegExpDomainSubset, StringDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		value = parent.cast(value);
		
		if (value instanceof NullValue)
			return NullValue.instance(this);
		
		String str = (String) value.get();
		
		if (regexp.test(str))
			return StringValue.of(str, this);
		else
			throw new VTLCastException(this, value); 
	}
	
	@Override
	public String toString()
	{
		return name + "{" + regexpstr + "}";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + name.hashCode();
		result = prime * result + parent.hashCode();
		result = prime * result + regexpstr.hashCode();
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
		RegExpDomainSubset other = (RegExpDomainSubset) obj;
		if (!name.equals(other.name))
			return false;
		if (!parent.equals(other.parent))
			return false;
		if (!regexpstr.equals(other.regexpstr))
			return false;
		return true;
	}

	@Override
	public Variable<RegExpDomainSubset, StringDomain> getDefaultVariable()
	{
		return new DefaultVariable<>(this);
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return parent.getRepresentation();
	}

	@Override
	public Class<?> getValueClass()
	{
		return StringValue.class;
	}
}
