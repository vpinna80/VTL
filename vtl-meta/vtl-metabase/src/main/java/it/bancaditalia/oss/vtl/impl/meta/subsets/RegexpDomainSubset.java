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

import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class RegexpDomainSubset implements StringDomainSubset<RegexpDomainSubset>, DescribedDomainSubset<RegexpDomainSubset, StringDomain>
{
	private static final long serialVersionUID = 1L;

	private final String name;
	private final StringDomainSubset<?> parent;
	private final Pattern regexp;
	
	public RegexpDomainSubset(String name, StringDomainSubset<?> parent, Pattern regexp)
	{
		this.name = name;
		this.parent = parent;
		this.regexp = regexp;
	}

	@Override
	public StringDomainSubset<?> getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, RegexpDomainSubset, StringDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> casted = parent.cast(value);
		
		if (casted instanceof NullValue)
			return NullValue.instance(this);
		
		if (regexp.matcher(value.toString()).matches())
			return new StringValue<>(value.toString(), this);
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof RegexpDomainSubset && parent.isAssignableFrom(other) && regexp.equals(((RegexpDomainSubset) other).regexp);
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return parent.isComparableWith(other);
	}

	@Override
	public Transformation getCriterion()
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public ScalarValue<?, ?, RegexpDomainSubset, StringDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}

	@Override
	public String getName()
	{
		return name + "{" + regexp + "}";
	}
	
	@Override
	public String toString()
	{
		return name + ":" + parent.getName();
	}
}
