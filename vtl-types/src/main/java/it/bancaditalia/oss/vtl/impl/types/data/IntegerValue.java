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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;

import java.util.Optional;
import java.util.OptionalLong;

import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;

public class IntegerValue<I extends IntegerValue<I, S>, S extends IntegerDomainSubset<S>> extends NumberValueImpl<I, Long, S, IntegerDomain>
{
	private static final long serialVersionUID = 1L;
	private static final ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> NULLINSTANCE = NullValue.instance(INTEGERDS);

	public IntegerValue(Long value, S domain)
	{
		super(value, domain);
	}
	
	public static ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> of(Long value)
	{
		return of(value, INTEGERDS);
	}

	public static <S extends IntegerDomainSubset<S>> ScalarValue<?, ?, S, IntegerDomain> of(Long value, S domain)
	{
		return value == null || value.longValue() == Long.MIN_VALUE ? NullValue.instance(domain) : new IntegerValue<>(value, domain);
	}
	
	public static ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> of(OptionalLong value)
	{
		return value != null && value.isPresent() ? of(value.getAsLong(), INTEGERDS) : NULLINSTANCE;
	}
	
	public static ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> of(Optional<Number> value)
	{
		return value != null && value.isPresent() ? of(value.get().longValue(), INTEGERDS) : NULLINSTANCE;
	}
	
	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof IntegerValue)
			return get().compareTo(((IntegerValue<?, ?>) o).get());
			
		return super.compareTo(o);
	}
}
