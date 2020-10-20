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

import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Map;

import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;

public class DurationValue extends BaseScalarValue<Double, DurationDomainSubset, DurationDomain>
{
	private static final long serialVersionUID = 1L;
	private static final Map<TemporalUnit, DurationDomains> DURATIONS_BY_UNIT = new HashMap<>(); 

	static 
	{
		for (DurationDomains duration: DurationDomains.values())
			DURATIONS_BY_UNIT.put(duration.getUnit(), duration);
	}

	private DurationValue(double value, DurationDomains duration)
	{
		super(value, duration);
	}

	@Override
	public ScalarValueMetadata<DurationDomainSubset> getMetadata()
	{
		return super::getDomain;
	}

	@Override
	public int compareTo(ScalarValue<?, ? extends ValueDomainSubset<?>, ? extends ValueDomain> o)
	{
		if (o instanceof DurationValue && o.getDomain().equals(getDomain()))
			return get().compareTo((Double) o.get());
		else
			throw new VTLCastException(getDomain(), o);
	}
}
