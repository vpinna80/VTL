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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;

import java.io.Serializable;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.date.OneHalf;
import it.bancaditalia.oss.vtl.impl.types.data.date.OneQuarter;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDurationDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;

public class DurationValue extends BaseScalarValue<DurationValue, Frequency, EntireDurationDomainSubset, DurationDomain> implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public static final TemporalAmount P1Y = Period.ofYears(1); 
	public static final TemporalAmount P1S = OneHalf.get(); 
	public static final TemporalAmount P1Q = OneQuarter.get(); 
	public static final TemporalAmount P1M = Period.ofMonths(1); 
	public static final TemporalAmount P1W = Period.ofWeeks(1); 
	public static final TemporalAmount P1D = Period.ofDays(1); 
	
	DurationValue(Frequency amount)
	{
		super(amount, DURATIONDS);
	}
	
	public static ScalarValue<?, ?, EntireDurationDomainSubset, DurationDomain> of(Frequency freq)
	{
		return freq != null ? freq.get() : null; 
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof DurationValue)
			return get().compareTo(((DurationValue) o).get());
		else
			throw new VTLCastException(DURATIONDS, o);
	}
}
