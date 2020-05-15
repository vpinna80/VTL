/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.types.domain;

import static it.bancaditalia.oss.vtl.impl.types.domain.Duration.A;
import static it.bancaditalia.oss.vtl.impl.types.domain.Duration.D;
import static it.bancaditalia.oss.vtl.impl.types.domain.Duration.M;
import static it.bancaditalia.oss.vtl.impl.types.domain.Duration.Q;
import static it.bancaditalia.oss.vtl.impl.types.domain.Duration.S;
import static it.bancaditalia.oss.vtl.impl.types.domain.Duration.W;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

class EntireTimePeriodDomainSubset<F extends PeriodHolder<?>> extends EntireDomainSubset<F, TimePeriodDomain> implements TimePeriodDomainSubset, Serializable
{
	public static final TimePeriodDomainSubset DAYSDS;
	public static final TimePeriodDomainSubset WEEKSDS;
	public static final TimePeriodDomainSubset MONTHSDS;
	public static final TimePeriodDomainSubset QUARTERSDS;
	public static final TimePeriodDomainSubset SEMESTERSDS;
	public static final TimePeriodDomainSubset YEARSDS;

	private static final Map<Duration, TimePeriodDomainSubset> PERIODSDS = new HashMap<>();
	private static final long serialVersionUID = 1L;
	
	static {
		for (Duration duration: Duration.values())
			PERIODSDS.put(duration, new EntireTimePeriodDomainSubset<>(duration));
		
		YEARSDS = PERIODSDS.get(A);
		SEMESTERSDS = PERIODSDS.get(S);
		QUARTERSDS = PERIODSDS.get(Q);
		MONTHSDS = PERIODSDS.get(M);
		WEEKSDS = PERIODSDS.get(W);
		DAYSDS = PERIODSDS.get(D);
	}
	
	private final Duration frequency;

	private EntireTimePeriodDomainSubset(Duration frequency)
	{
		super(PERIODSDS.get(frequency));
		this.frequency = frequency;
	}
	
	public static TimePeriodDomainSubset of(Duration frequency)
	{
		return PERIODSDS.get(frequency);
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
	public TimePeriodValue cast(ScalarValue<?, ?, ?> value)
	{
		if (value instanceof TimePeriodValue)
			return (TimePeriodValue) value;
		else
			throw new VTLCastException(this, value);
	}
	
	@Override
	public String toString()
	{
		return "Time_Period(" + frequency + ")";
	}
}