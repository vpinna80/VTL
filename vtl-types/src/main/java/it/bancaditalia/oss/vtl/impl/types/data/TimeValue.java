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

import java.io.Serializable;
import java.time.temporal.TemporalAccessor;

import it.bancaditalia.oss.vtl.impl.types.data.DurationValue.Durations;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.DayPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.MonthPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.QuarterPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.SemesterPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.WeekPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue.YearPeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DayPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.WeekPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;

public abstract class TimeValue<I extends TimeValue<I, R, S, D>, R extends Comparable<? super R> & TemporalAccessor & Serializable & TimeHolder, S extends TimeDomainSubset<S, D>, D extends TimeDomain> 
		extends BaseScalarValue<I, R, S, D>
{
	private static final long serialVersionUID = 1L;

	public TimeValue(R value, S domain)
	{
		super(value, domain);
	}

	public abstract TimeValue<?, ?, ?, ?> increment(long amount);
	
	public TimePeriodValue<?, ?> wrap(Durations duration)
	{
		switch (duration)
		{
			case A: return new YearPeriodValue(new YearPeriodHolder(get()));
			case H: return new SemesterPeriodValue(new SemesterPeriodHolder(get()));
			case Q: return new QuarterPeriodValue(new QuarterPeriodHolder(get()));
			case M: return new MonthPeriodValue(new MonthPeriodHolder(get()));
			case W: return new WeekPeriodValue(new WeekPeriodHolder(get()));
			case D: return new DayPeriodValue(new DayPeriodHolder(get()));
			default: throw new IllegalStateException(); // Should never occur
		}
	}
}
