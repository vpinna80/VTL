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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValueImpl.TimeHolderImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;

class EntireTimeDomainSubset extends EntireDomainSubset<TimeHolderImpl, TimeDomain> implements TimeDomainSubset<TimeDomain>, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final TimeDomainSubset<TimeDomain> INSTANCE = new EntireTimeDomainSubset();

	public static TimeDomainSubset<TimeDomain> getInstance()
	{
		return INSTANCE;
	}

	private EntireTimeDomainSubset()
	{
		super(TIMEDS, "time_var");
	}

	@Override
	public String toString()
	{
		return "time";
	}
	
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof TimeDomainSubset;
	}

	@Override
	public ScalarValue<?, ?, ? extends TimeDomainSubset<? extends TimeDomain>, ? extends TimeDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (isAssignableFrom(value.getDomain()) && value instanceof NullValue)
			return NullValue.instance(this);
		else if (value instanceof TimeValue)
			return (TimeValue<?, ?, ?, ?>) value;
		throw new UnsupportedOperationException("Cast to time from " + value.getClass());
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return other instanceof DateDomain;
	}
}
