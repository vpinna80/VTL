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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import java.time.Period;
import java.time.temporal.TemporalAmount;

import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimeDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;

public class GenericTimeValue<S extends TimeDomainSubset<S, TimeDomain>> extends TimeValue<GenericTimeValue<S>, TimeRangeHolder, S, TimeDomain>
{
	private GenericTimeValue(TimeRangeHolder value, S domain)
	{
		super(value, domain);
	}

	public static ScalarValue<?, ?, EntireTimeDomainSubset, TimeDomain> of(TimeRangeHolder value)
	{
		return value == null ? NullValue.instance(TIMEDS) : new GenericTimeValue<>(value, TIMEDS);
	}
	
	public static ScalarValue<?, ?, EntireTimeDomainSubset, TimeDomain> of(
			ScalarValue<?, ?, ? extends TimeDomainSubset<?, ? extends TimeDomain>, ? extends TimeDomain> start, 
			ScalarValue<?, ?, ? extends TimeDomainSubset<?, ? extends TimeDomain>, ? extends TimeDomain> endInclusive)
	{
		if (start == null || endInclusive == null || start.isNull() || endInclusive.isNull()) 
			return NullValue.instance(TIMEDS);
		
		return new GenericTimeValue<>(new TimeRangeHolder((TimeValue<?, ?, ?, ?>) start, (TimeValue<?, ?, ?, ?>) endInclusive), TIMEDS);
	}
	
	private static final long serialVersionUID = 1L;

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		return get().compareTo(((GenericTimeValue<?>) o).get());
	}

	@Override
	public GenericTimeValue<S> add(long amount)
	{
		return new GenericTimeValue<>(get().incrementSmallest(amount), getDomain());
	}

	@Override
	public GenericTimeValue<S> add(TemporalAmount period)
	{
		return new GenericTimeValue<>(get().increment(period), getDomain());
	}

	@Override
	public GenericTimeValue<S> minus(TemporalAmount period)
	{
		return new GenericTimeValue<>(get().decrement(period), getDomain());
	}
	
	@Override
	public Period until(TimeValue<?, ?, ?, ?> endInclusive)
	{
		return get().until(endInclusive);
	}

	@Override
	public DurationValue getFrequency()
	{
		return get().getFrequency();
	}

	@Override
	public DateValue<?> getStartDate()
	{
		return get().getStartDate();
	}

	@Override
	public DateValue<?> getEndDate()
	{
		return get().getEndDate();
	}
}
