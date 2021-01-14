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

import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;

public abstract class TimeValue<R extends Comparable<? super R> & TemporalAccessor & Serializable & TimeHolder, S extends TimeDomainSubset<D>, D extends TimeDomain> 
		extends BaseScalarValue<R, S, D>
{
	private static final long serialVersionUID = 1L;

	public TimeValue(R value, S domain)
	{
		super(value, domain);
	}

	public abstract TimeValue<?, ?, ?> increment(long amount);
	
	public TimePeriodValue wrap(DurationDomains frequency)
	{
		return get().wrap(frequency);
	}
}
