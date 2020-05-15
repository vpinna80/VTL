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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;

class EntireDurationDomainSubset extends EntireDomainSubset<String, DurationDomain> implements DurationDomainSubset
{
	private static final long serialVersionUID = 1L;

	public EntireDurationDomainSubset()
	{
		super(DURATIONDS);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof DurationDomainSubset;
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return other instanceof DurationDomainSubset;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ScalarValue<?, DurationDomainSubset, DurationDomain> cast(ScalarValue<?, ?, ?> value)
	{
		if (isAssignableFrom(value.getDomain()))
			if (value instanceof NullValue)
				return NullValue.instance(this);
			else
				return (ScalarValue<?, DurationDomainSubset, DurationDomain>) value;
		else
			throw new VTLCastException(DURATIONDS, value);
	}
	
	@Override
	public String toString()
	{
		return "duration";
	}
	
	@Override
	public String getVarName()
	{
		return "duration_var";
	}
}
