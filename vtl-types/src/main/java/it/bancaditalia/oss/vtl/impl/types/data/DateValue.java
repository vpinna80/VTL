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
import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;

public class DateValue extends TimeValue<DateValue, DateHolder<?>, DateDomainSubset, DateDomain>
{
	private static final long serialVersionUID = 1L;

	public DateValue(TemporalAccessor value)
	{
		this(DateHolder.of(value));
	}

	public DateValue(DateHolder<?> value)
	{
		super(value, DATEDS);
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof DateValue)
			return get().compareTo(((DateValue) o).get());
		else
			throw new UnsupportedOperationException();
	}

	@Override
	public ScalarValueMetadata<DateDomainSubset> getMetadata()
	{
		return () -> DATEDS;
	}

	@Override
	public DateValue increment(long amount)
	{
		return new DateValue(get().increment(amount));
	}

	@Override
	public TimePeriodValue wrap(DurationDomains frequency)
	{
		return get().wrap(frequency);
	}
}
