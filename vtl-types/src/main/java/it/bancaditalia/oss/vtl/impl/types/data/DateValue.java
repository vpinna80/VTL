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
package it.bancaditalia.oss.vtl.impl.types.data;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;

import java.time.temporal.TemporalAccessor;

import it.bancaditalia.oss.vtl.impl.types.data.date.DateHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDateDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;

public class DateValue<S extends DateDomainSubset<S>> extends TimeValue<DateValue<S>, DateHolder<?>, S, DateDomain>
{
	private static final long serialVersionUID = 1L;
	private static final ScalarValue<?, ?, EntireDateDomainSubset, DateDomain> NULLINSTANCE = NullValue.instance(DATEDS);

	public DateValue(DateHolder<?> value, S domain)
	{
		super(value, domain);
	}
	
	public static ScalarValue<?, ?, ? extends DateDomainSubset<?>, ? extends DateDomain> of(TemporalAccessor value)
	{
		return value == null ? NULLINSTANCE : new DateValue<>(DateHolder.of(value), DATEDS);
	}
	
	public static ScalarValue<?, ?, ? extends DateDomainSubset<?>, ? extends DateDomain> of(DateHolder<?> value)
	{
		return value == null ? NULLINSTANCE : new DateValue<>(value, DATEDS);
	}

	public <S1 extends DateDomainSubset<S1>> DateValue<S1> as(S1 domain2)
	{
		return new DateValue<>(get(), domain2);
	}
	
	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof DateValue)
			return get().compareTo(((DateValue<?>) o).get());
		else
			throw new UnsupportedOperationException();
	}

	@Override
	public DateValue<S> increment(long amount)
	{
		return new DateValue<>(get().increment(amount), getDomain());
	}
}
