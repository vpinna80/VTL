/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.types.domain;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;

class EntireBooleanDomainSubset extends EntireDomainSubset<Boolean, BooleanDomain> implements BooleanDomainSubset, Serializable
{
	private static final long serialVersionUID = 1L;

	EntireBooleanDomainSubset()
	{ 
		super(Domains.BOOLEANDS);
	}
	
	@Override
	public String toString()
	{
		return "boolean";
	}
	
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof BooleanDomainSubset || 
				other instanceof NumberDomainSubset ||
				(other instanceof ValueDomainSubset && isAssignableFrom(((ValueDomainSubset<?>) other).getParentDomain()));
	}

	@Override
	public ScalarValue<Boolean, BooleanDomainSubset, BooleanDomain> cast(ScalarValue<?, ?, ?> value)
	{
		if (isAssignableFrom(value.getDomain()) && value instanceof NullValue)
			return NullValue.instance(this);
		else if (value instanceof BooleanValue)
			return (BooleanValue) value;
		else if (value instanceof NumberValue)
			return BooleanValue.of(((NumberValue<?, ?, ?>) value).get().doubleValue() != 0);
		else
			throw new VTLCastException(BOOLEANDS, value);
	}
}
