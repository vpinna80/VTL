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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;

import java.util.OptionalDouble;

import it.bancaditalia.oss.vtl.impl.types.domain.EntireNumberDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;

public class DoubleValue<S extends NumberDomainSubset<S, NumberDomain>> extends NumberValueImpl<DoubleValue<S>, Double, S, NumberDomain>
{
	private static final long serialVersionUID = 1L;
	private static final ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> NULLINSTANCE = NullValue.instance(NUMBERDS);

	@SuppressWarnings("unchecked")
	public static final DoubleValue<EntireNumberDomainSubset> ZERO = (DoubleValue<EntireNumberDomainSubset>) DoubleValue.of(0.0);

	private DoubleValue(Double value, S domain)
	{
		super(value, domain);
	}
	
	static ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> of(Double value)
	{
		return value == null ? NULLINSTANCE : new DoubleValue<>(value, NUMBERDS);
	}

	public static ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> of(OptionalDouble value)
	{
		return value != null && value.isPresent() ? new DoubleValue<>(value.getAsDouble(), NUMBERDS) : NULLINSTANCE;
	}

	public static <S extends NumberDomainSubset<S, NumberDomain>> ScalarValue<?, ?, S, NumberDomain> of(Double value, S domain)
	{
		return value == null ? NullValue.instance(domain) : new DoubleValue<>(value, domain);
	}

	public static <S extends NumberDomainSubset<S, NumberDomain>> ScalarValue<?, ?, S, NumberDomain> of(OptionalDouble value, S domain)
	{
		return value != null && value.isPresent() ? new DoubleValue<>(value.getAsDouble(), domain) : NullValue.instance(domain);
	}
}
