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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;

import java.io.Serializable;
import java.time.LocalDateTime;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;

public class EntireDateDomainSubset extends EntireDomainSubset<LocalDateTime, DateDomain> implements DateDomainSubset, Serializable
{
	private static final long serialVersionUID = 1L;

	public EntireDateDomainSubset()
	{
		super(DATEDS, "date_var");
	}

	@Override
	public String toString()
	{
		return "date";
	}
	
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof NumberDomainSubset;
	}

	@Override
	public ScalarValue<?, DateDomainSubset, DateDomain> cast(ScalarValue<?, ?, ?> value)
	{
		if (isAssignableFrom(value.getDomain()) && value instanceof NullValue)
			return NullValue.instance(this);
		if (value instanceof DateValue)
			return (DateValue) value;
		else
			throw new UnsupportedOperationException("Cast to date from " + value.getDomain());
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return other instanceof DateDomain;
	}
}
