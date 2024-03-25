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

import java.math.BigDecimal;

import it.bancaditalia.oss.vtl.impl.types.domain.EntireNumberDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;

public class BigDecimalValue<S extends NumberDomainSubset<S, NumberDomain>> extends NumberValueImpl<BigDecimalValue<S>, BigDecimal, S, NumberDomain>
{
	private static final long serialVersionUID = 1L;
	private static final ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> NULLINSTANCE = NullValue.instance(NUMBERDS);

	BigDecimalValue(BigDecimal value, S domain)
	{
		super(value, domain);
	}
	
	public static ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> of(BigDecimal value)
	{
		return value == null ? NULLINSTANCE : new BigDecimalValue<>(value, NUMBERDS);
	}

	public static <S extends NumberDomainSubset<S, NumberDomain>> ScalarValue<?, ?, S, NumberDomain> of(BigDecimal value, S domain)
	{
		return value == null ? NullValue.instance(domain) : new BigDecimalValue<>(value, domain);
	}
}
