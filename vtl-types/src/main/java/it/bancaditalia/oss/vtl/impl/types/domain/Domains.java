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

import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

@SuppressWarnings("rawtypes")
public enum Domains implements ScalarValueMetadata
{
	BOOLEAN(new EntireBooleanDomainSubset()),
	INTEGER(new EntireIntegerDomainSubset()),
	DURATION(EntireDurationDomainSubset.INSTANCE),
	STRING(new EntireStringDomainSubset()),
	NUMBER(new EntireNumberDomainSubset()),
	TIME(EntireTimeDomainSubset.getInstance()),
	DATE(EntireDateDomainSubset.getInstance()),
	TIME_PERIOD(EntireTimePeriodDomainSubset.getInstance()),
	NULL(new NullDomain());
	
	public static final EntireDurationDomainSubset DURATIONDS = (EntireDurationDomainSubset) DURATION.getDomain();
	public static final EntireNumberDomainSubset NUMBERDS = (EntireNumberDomainSubset) NUMBER.getDomain();
	public static final EntireIntegerDomainSubset INTEGERDS = (EntireIntegerDomainSubset) INTEGER.getDomain();
	public static final EntireBooleanDomainSubset BOOLEANDS = (EntireBooleanDomainSubset) BOOLEAN.getDomain();
	public static final EntireStringDomainSubset STRINGDS = (EntireStringDomainSubset) STRING.getDomain();
	public static final EntireDateDomainSubset DATEDS = (EntireDateDomainSubset) DATE.getDomain();
	public static final EntireTimeDomainSubset TIMEDS = (EntireTimeDomainSubset) TIME.getDomain();
	public static final EntireTimePeriodDomainSubset TIME_PERIODDS = (EntireTimePeriodDomainSubset) TIME_PERIOD.getDomain();
	public static final NullDomain NULLDS = (NullDomain) NULL.getDomain();

	private final ValueDomainSubset<?, ? extends ValueDomain> valueDomain;
	
	private Domains(ValueDomainSubset<?, ?> valueDomain)
	{
		this.valueDomain = valueDomain;
	}
	
	public boolean isAssignableFrom(ScalarValueMetadata<?, ?> other)
	{
		return getDomain().isAssignableFrom(other.getDomain());
	}

	public boolean isAssignableFrom(ValueDomainSubset<?, ?> other)
	{
		return getDomain().isAssignableFrom(other);
	}

	public ValueDomainSubset<?, ?> getDomain()
	{
		return valueDomain;
	}
}
