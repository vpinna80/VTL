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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.MONTHSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.QUARTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.WEEKSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.YEARSDS;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DayPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public class EntireTimePeriodDomainSubset extends EntireDomainSubset<PeriodHolder<?>, TimePeriodDomain> implements TimePeriodDomainSubset, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Map<Class<? extends PeriodHolder<?>>, TimePeriodDomainSubset> PARENTS = new HashMap<>();
	
	static {
		PARENTS.put(YearPeriodHolder.clazz(), null);
		PARENTS.put(SemesterPeriodHolder.class, YEARSDS);
		PARENTS.put(QuarterPeriodHolder.class, QUARTERSDS);
		PARENTS.put(MonthPeriodHolder.class, MONTHSDS);
		PARENTS.put(WeekPeriodHolder.class, WEEKSDS);
		PARENTS.put(DayPeriodHolder.class, MONTHSDS);
	}
	
	public EntireTimePeriodDomainSubset(Class<? extends PeriodHolder<?>> holder, String defaultVarName)
	{
		super(PARENTS.get(holder), defaultVarName);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return this == other;
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public ScalarValue<?, ? extends ValueDomainSubset<? extends TimePeriodDomain>, ? extends TimePeriodDomain> cast(ScalarValue<?, ?, ?> value)
	{
		if (value instanceof TimePeriodValue)
			return (TimePeriodValue) value;
		else
			throw new VTLCastException(this, value);
	}
	
	@Override
	public String toString()
	{
		return "time_period";
	}
}