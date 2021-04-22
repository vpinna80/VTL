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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DAYSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.MONTHSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.QUARTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.SEMESTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.WEEKSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.YEARSDS;

import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.ValueRange;
import java.util.HashMap;
import java.util.Map;

import it.bancaditalia.oss.vtl.impl.types.data.date.DayPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.VTLTimePatterns;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public class TimePeriodValue<S extends TimePeriodDomainSubset<S>> extends TimeValue<TimePeriodValue<S>, PeriodHolder<?>, S, TimePeriodDomain> implements TemporalAccessor
{
	private static final long serialVersionUID = 1L;

	private static final Map<Class<?>, TimePeriodDomainSubset<?>> DOMAINS_MAP = new HashMap<>();
	
	static 
	{
		DOMAINS_MAP.put(DayPeriodHolder.class, DAYSDS);
		DOMAINS_MAP.put(WeekPeriodHolder.class, WEEKSDS);
		DOMAINS_MAP.put(MonthPeriodHolder.class, MONTHSDS);
		DOMAINS_MAP.put(QuarterPeriodHolder.class, QUARTERSDS);
		DOMAINS_MAP.put(SemesterPeriodHolder.class, SEMESTERSDS);
		DOMAINS_MAP.put(YearPeriodHolder.class, YEARSDS);
	}
	
	public static TimePeriodValue<?> of(PeriodHolder<?> holder)
	{
		TimePeriodDomainSubset<?> domain = DOMAINS_MAP.get(holder.getClass());
		@SuppressWarnings({ "rawtypes", "unchecked" })
		final TimePeriodValue<?> result = (TimePeriodValue<?>) new TimePeriodValue(holder, domain);
		return result;
	}
	
	public static TimePeriodValue<?> of(String string, String mask)
	{
		DateTimeFormatter formatter = VTLTimePatterns.getFormatter(mask);
		PeriodHolder<?> holder = (PeriodHolder<?>) formatter.parseBest(string, DayPeriodHolder::new, WeekPeriodHolder::new, 
				MonthPeriodHolder::new, QuarterPeriodHolder::new, SemesterPeriodHolder::new, YearPeriodHolder::new);
		return of(holder);
	}

	public TimePeriodValue(PeriodHolder<?> value, S domain)
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
	public TimePeriodValue<S> increment(long amount)
	{
		return new TimePeriodValue<>(get().incrementSmallest(amount), getDomain());
	}
	
	public static String getQualifier(Class<? extends PeriodHolder<?>> holder)
	{
		if (DayPeriodHolder.class.isAssignableFrom(holder))
			return "P1D";
		if (WeekPeriodHolder.class.isAssignableFrom(holder))
			return "P1W";
		if (MonthPeriodHolder.class.isAssignableFrom(holder))
			return "P1M";
		if (QuarterPeriodHolder.class.isAssignableFrom(holder))
			return "P1Q";
		if (SemesterPeriodHolder.class.isAssignableFrom(holder))
			return "P1S";
		if (YearPeriodHolder.class.isAssignableFrom(holder))
			return "P1Y";
		throw new UnsupportedOperationException("Unknown class " + holder);
	}
	
	public String getPeriodIndicator()
	{
		return null;
	}

	public boolean isSupported(TemporalField field)
	{
		return get().isSupported(field);
	}

	public ValueRange range(TemporalField field)
	{
		return get().range(field);
	}

	public int get(TemporalField field)
	{
		return get().get(field);
	}

	public long getLong(TemporalField field)
	{
		return get().getLong(field);
	}

	public <Q> Q query(TemporalQuery<Q> query)
	{
		return get().query(query);
	}
	
}
