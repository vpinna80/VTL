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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;

import java.io.Serializable;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

/**
 * Must be in period-length order 
 */
public class EntireDurationDomainSubset extends EntireDomainSubset<EntireDurationDomainSubset, DurationValue, EntireDurationDomainSubset, DurationDomain> implements DurationDomainSubset<EntireDurationDomainSubset>, Serializable
{
	private static final long serialVersionUID = 1L;
	public static final EntireDurationDomainSubset INSTANCE = new EntireDurationDomainSubset();
	
	private EntireDurationDomainSubset()
	{
		super(DURATIONDS);
	}
	
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof NullDomain || other instanceof DurationDomain || other instanceof StringDomain;
	}

	@Override
	public ScalarValue<?, ?, EntireDurationDomainSubset, DurationDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value.isNull())
			return NullValue.instance(INSTANCE);
		else if (value instanceof DurationValue)
			return (DurationValue) value;
		else if (value instanceof StringValue && Set.of("A", "S", "Q", "M", "W", "D").contains(value.get()))
			return Frequency.valueOf((String) value.get()).get();
		else
			throw new VTLCastException(INSTANCE, value);
	}
	
	@Override
	public String toString()
	{
		return "duration";
	}
	
	@Override
	public int hashCode()
	{
		return getClass().hashCode();
	}
	
	@Override
	public boolean equals(Object obj)
	{
		return obj != null && obj.getClass() == getClass();
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
		return Frequency.class;
	}
	
	@Override
	public Class<?> getValueClass()
	{
		return DurationValue.class;
	}
}