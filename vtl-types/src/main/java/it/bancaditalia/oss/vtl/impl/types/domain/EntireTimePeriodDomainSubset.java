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

import java.io.Serializable;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

public class EntireTimePeriodDomainSubset extends EntireDomainSubset<EntireTimePeriodDomainSubset, TimePeriodValue<EntireTimePeriodDomainSubset>, EntireTimePeriodDomainSubset, TimePeriodDomain> implements TimePeriodDomainSubset<EntireTimePeriodDomainSubset>
{
	private static final long serialVersionUID = 1L;
	private static final EntireTimePeriodDomainSubset INSTANCE = new EntireTimePeriodDomainSubset(null);
	
	public static EntireTimePeriodDomainSubset getInstance()
	{
		return INSTANCE;
	}
	
	private EntireTimePeriodDomainSubset(TimePeriodDomain parentDomain)
	{
		super(parentDomain);
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		throw new UnsupportedOperationException();
	}

	public ScalarValue<?, ?, EntireTimePeriodDomainSubset, TimePeriodDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value.isNull())
			return NullValue.instance(this);
		else if (value instanceof TimePeriodValue)
			return TimePeriodValue.of((PeriodHolder<?>) value.get(), this);
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof NullDomain || other instanceof TimePeriodDomainSubset; 
	}
	
	@Override
	public String toString()
	{
		return "time_period";
	}

	@Override
	public Class<? extends Serializable> getRepresentation()
	{
//		throw new UnsupportedOperationException("time_period does not have a single default representation.");
		return PeriodHolder.class; 
	}

	@Override
	public Class<?> getValueClass()
	{
		return TimePeriodValue.class;
	}
}