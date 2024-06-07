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

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class EntireStringDomainSubset extends EntireDomainSubset<EntireStringDomainSubset, StringValue<?, EntireStringDomainSubset>, EntireStringDomainSubset, StringDomain> implements StringDomainSubset<EntireStringDomainSubset>, Serializable
{
	private static final long serialVersionUID = 1L;

	EntireStringDomainSubset()
	{
		super(STRINGDS);
	}

	@Override
	public String toString()
	{
		return "string";
	}
	
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof NullDomain || other instanceof StringDomainSubset || other instanceof IntegerDomainSubset || other instanceof BooleanDomainSubset;
	}

	@Override
	public ScalarValue<?, ?, EntireStringDomainSubset, StringDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof NullValue)
			return NullValue.instance(this);
		else if (value instanceof StringValue)
			return StringValue.of(value.get().toString());
		else if (isAssignableFrom(value.getDomain()))
			return StringValue.of(value.toString().toUpperCase());
		else 
			throw new VTLCastException(this, value);
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return String.class; 
	}
}
