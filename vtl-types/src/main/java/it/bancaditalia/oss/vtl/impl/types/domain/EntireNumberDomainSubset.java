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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;

import java.io.Serializable;
import java.math.BigDecimal;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class EntireNumberDomainSubset extends EntireDomainSubset<EntireNumberDomainSubset, NumberValue<?, ?, EntireNumberDomainSubset, NumberDomain>, EntireNumberDomainSubset, NumberDomain> implements NumberDomainSubset<EntireNumberDomainSubset, NumberDomain>, Serializable
{
	private static final long serialVersionUID = 1L;

	EntireNumberDomainSubset()
	{
		super(NUMBERDS);
	}

	@Override
	public String toString()
	{
		return "number";
	}
	
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof NullDomain || other instanceof NumberDomainSubset;
	}

	@Override
	public ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (isAssignableFrom(value.getDomain()))
		{
			if (value instanceof NullValue)
				return NullValue.instance(this);
			else if (value instanceof NumberValue)
				return createNumberValue((Number) value.get());
			else 
				throw new UnsupportedOperationException("Cast to double from " + value.getClass());
		}
		else 
			throw new VTLCastException(this, value);
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return isUseBigDecimal() ? BigDecimal.class : Double.class;
	}

	@Override
	public Class<?> getValueClass()
	{
		return isUseBigDecimal() ? BigDecimalValue.class : DoubleValue.class;
	}
}
