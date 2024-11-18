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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME_PERIODDS;

import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAmount;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.VTLTimePatterns;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public class TimePeriodValue<S extends TimePeriodDomainSubset<S>> extends TimeValue<TimePeriodValue<S>, PeriodHolder<?>, S, TimePeriodDomain>
{
	private static final long serialVersionUID = 1L;

	public static TimePeriodValue<EntireTimePeriodDomainSubset> of(PeriodHolder<?> holder)
	{
		return of(holder, TIME_PERIODDS);
	}
	
	@SuppressWarnings("unchecked")
	public static <S extends TimePeriodDomainSubset<S>> TimePeriodValue<S> of(PeriodHolder<?> holder, TimePeriodDomainSubset<S> domain)
	{
		return new TimePeriodValue<>(holder, (S) domain);
	}

	public static TimePeriodValue<?> of(String string, String mask)
	{
		DateTimeFormatter formatter = VTLTimePatterns.getFormatter(mask);
		PeriodHolder<?> holder = (PeriodHolder<?>) formatter.parseBest(string, WeekPeriodHolder::new, 
				MonthPeriodHolder::new, QuarterPeriodHolder::new, SemesterPeriodHolder::new, YearPeriodHolder::new);
		return of(holder);
	}

	private TimePeriodValue(PeriodHolder<?> value, S domain)
	{
		super(value, domain);
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof TimePeriodValue)
			return this.get().compareTo(((TimePeriodValue<?>) o).get());
		else
			throw new VTLIncompatibleTypesException("compare", getDomain(), o.getDomain());
	}

	@Override
	public TimePeriodValue<S> add(long amount)
	{
		return new TimePeriodValue<>(get().incrementSmallest(amount), getDomain());
	}
	
	@Override
	public TimePeriodValue<S> add(TemporalAmount period)
	{
		return new TimePeriodValue<>(get().increment(period), getDomain());
	}
	
	@Override
	public TimePeriodValue<S> minus(TemporalAmount period)
	{
		return new TimePeriodValue<>(get().decrement(period), getDomain());
	}

	@Override
	public Period until(TimeValue<?, ?, ?, ?> end)
	{
		return get().until(end);
	}
	
	@Override
	public DurationValue getFrequency()
	{
		return get().getPeriodIndicator().get();
	}

	@Override
	public DateValue<?> getStartDate()
	{
		return (DateValue<?>) DateValue.of(get().startDate());
	}

	@Override
	public DateValue<?> getEndDate()
	{
		return (DateValue<?>) DateValue.of(get().endDate());
	}
}
