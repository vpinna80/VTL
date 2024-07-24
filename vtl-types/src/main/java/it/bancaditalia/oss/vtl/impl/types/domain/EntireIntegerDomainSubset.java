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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class EntireIntegerDomainSubset extends EntireDomainSubset<EntireIntegerDomainSubset, IntegerValue<?, EntireIntegerDomainSubset>, EntireIntegerDomainSubset, IntegerDomain> implements IntegerDomainSubset<EntireIntegerDomainSubset>, Serializable
{
	private static final long serialVersionUID = 1L;

	EntireIntegerDomainSubset()
	{
		super(INTEGERDS);
	}

	@Override
	public String toString()
	{
		return "integer";
	}
	
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof NullDomain || other instanceof IntegerDomainSubset;
	}

	@Override
	public ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (isAssignableFrom(value.getDomain()))
		{
			if (value instanceof NullValue)
				return NullValue.instance(this);
//			else if (value instanceof BooleanValue)
//				return IntegerValue.of(((BooleanValue<?>) value) == BooleanValue.TRUE ? 1L : 0);
			else if (value instanceof DoubleValue || value instanceof BigDecimalValue)
				return IntegerValue.of(((Number) value.get()).longValue());
			else if (value instanceof IntegerValue)
				return IntegerValue.of((Long) value.get());
//			else if (value instanceof StringValue)
//				return IntegerValue.of(Long.parseLong(value.get().toString()));
			else 
				throw new VTLCastException(this, value);
		}
		else 
			throw new VTLCastException(this, value);
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return Long.class;
	}
	
	@Override
	public Class<?> getValueClass()
	{
		return IntegerValue.class;
	}
}
