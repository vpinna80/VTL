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

import java.time.temporal.TemporalAccessor;

import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public class TimePeriodValue extends TimeValue<PeriodHolder<?>, TimePeriodDomainSubset, TimePeriodDomain>
{
	private static final long serialVersionUID = 1L;

	public TimePeriodValue(TemporalAccessor value)
	{
		this(PeriodHolder.of(value));
	}

	private TimePeriodValue(PeriodHolder<?> holder)
	{
		super(holder, holder.getDomain());
	}

	@Override
	public int compareTo(ScalarValue<?, ? extends ValueDomainSubset<?>, ? extends ValueDomain> o)
	{
		if (o instanceof TimePeriodValue)
			return this.get().compareTo(((TimePeriodValue) o).get());
		else
			throw new VTLIncompatibleTypesException("compare", getDomain(), o.getDomain());
	}

	@Override
	public ScalarValueMetadata<? extends TimePeriodDomainSubset> getMetadata()
	{
		return get()::getDomain;
	}

	@Override
	public TimePeriodValue increment(long amount)
	{
		PeriodHolder<?> holder = get();
		return new TimePeriodValue(holder.incrementSmallest(amount));
	}
}
