/**
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

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;

import it.bancaditalia.oss.vtl.impl.types.data.TimeValueImpl.TimeHolderImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;

public class TimeValueImpl extends TimeValue<TimeHolderImpl, TimeDomainSubset<TimeDomain>, TimeDomain>
{
	private static final long serialVersionUID = 1L;

	public static class TimeHolderImpl implements Comparable<TimeHolder>, Serializable, TemporalAccessor, TimeHolder
	{
		private static final long serialVersionUID = 1L;

		public TimeHolderImpl(LocalDateTime start, LocalDateTime end)
		{

		}

		@Override
		public int compareTo(TimeHolder other)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public long getLong(TemporalField field)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isSupported(TemporalField field)
		{
			throw new UnsupportedOperationException();
		}

		public String getPeriod()
		{
			throw new UnsupportedOperationException();
		}

		public TimePeriodValue wrap(DurationDomains frequency)
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public String getPeriodIndicator()
		{
			throw new UnsupportedOperationException();
		}
	}
	
	private TimeValueImpl(LocalDateTime start, LocalDateTime end)
	{
		super(new TimeHolderImpl(start, end), TIMEDS);
	}

	@Override
	public int compareTo(ScalarValue<?, ? extends ValueDomainSubset<?>, ? extends ValueDomain> o)
	{
		if (o instanceof TimeValueImpl)
			return get().compareTo(((TimeValueImpl) o).get());
		else
			throw new UnsupportedOperationException();
	}

	@Override
	public ScalarValueMetadata<TimeDomainSubset<TimeDomain>> getMetadata()
	{
		return () -> TIMEDS;
	}

	@Override
	public TimeValue<?, ?, ?> increment(long amount)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public TimePeriodValue wrap(DurationDomains frequency)
	{
		return get().wrap(frequency);
	}
}
