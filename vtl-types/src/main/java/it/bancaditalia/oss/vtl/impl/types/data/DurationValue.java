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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.MONTHSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.QUARTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.SEMESTERSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.WEEKSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.YEARSDS;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.date.DurationHolder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDurationDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;

public class DurationValue<S extends DurationDomainSubset<S>> extends BaseScalarValue<DurationValue<S>, DurationHolder, S, DurationDomain>
{
	private static final long serialVersionUID = 1L;
	public static enum Durations
	{
		A(YEARSDS), H(SEMESTERSDS), Q(QUARTERSDS), M(MONTHSDS), W(WEEKSDS), D(null);

		private final TimePeriodDomainSubset<?> related;

		Durations(TimePeriodDomainSubset<?> related)
		{
			this.related = related;
		}

		public TimePeriodDomainSubset<?> getRelatedTimePeriodDomain()
		{
			return related;
		}
	}
	
	private DurationValue(DurationHolder holder, S domain)
	{
		super(holder, domain);
	}

	public static DurationValue<EntireDurationDomainSubset> of(double value, Durations duration)
	{
		return new DurationValue<>(new DurationHolder(value, duration), DURATIONDS);
	}

	public static DurationValue<EntireDurationDomainSubset> of(DurationHolder holder)
	{
		return new DurationValue<>(holder, DURATIONDS);
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof DurationValue)
			return get().compareTo((DurationHolder) o.get());
		else
			throw new VTLCastException(getDomain(), o);
	}
}
