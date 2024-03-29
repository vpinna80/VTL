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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;

import java.math.BigDecimal;
import java.util.Objects;

import it.bancaditalia.oss.vtl.impl.types.domain.EntireNumberDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;

public abstract class NumberValueImpl<T extends NumberValueImpl<T, R, S, D>, R extends Number & Comparable<? super R>, S extends NumberDomainSubset<S, D>, D extends NumberDomain> 
	extends BaseScalarValue<T, R, S, D> implements NumberValue<T, R, S, D>
{
	private static final long serialVersionUID = 1L;

	public static <S extends NumberDomainSubset<S, NumberDomain>> ScalarValue<?, ?, S, NumberDomain> createNumberValue(Number n, S domain)
	{
		if (isUseBigDecimal())
			return BigDecimalValue.of(n == null || n instanceof BigDecimal ? (BigDecimal) n : new BigDecimal(n.doubleValue()), domain);
		else
			return DoubleValue.of(n == null || n instanceof Double ? (Double) n : n.doubleValue(), domain);
	}
	
	public static ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> createNumberValue(Number n)
	{
		return createNumberValue(n, NUMBERDS);
	}
	
	public static ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> createNumberValue(String s)
	{
		if (isUseBigDecimal())
			return BigDecimalValue.of(s != null ? new BigDecimal(s) : null);
		else
			return DoubleValue.of(s != null ? Double.parseDouble(s) : null);
	}
	
	public NumberValueImpl(R value, S domain)
	{
		super(Objects.requireNonNull(value), domain);
	}
	
	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o instanceof NullValue)
			throw new NullPointerException("Comparison with null values");
		else if (this instanceof IntegerValue && o instanceof IntegerValue)
			return Long.valueOf(get().longValue()).compareTo(Long.valueOf(((NumberValueImpl<?, ?, ?, ?>) o).get().longValue()));
		else if (this instanceof DoubleValue || o instanceof DoubleValue)
			return Double.compare(get().doubleValue(), ((NumberValueImpl<?, ?, ?, ?>) o).get().doubleValue());
		else if (this instanceof BigDecimalValue && o instanceof BigDecimalValue)
			return ((BigDecimal) get()).compareTo((BigDecimal) o.get());
		else if (this instanceof BigDecimalValue && o instanceof IntegerValue)
			return ((BigDecimal) get()).compareTo(new BigDecimal((Long) o.get()));
		else if (this instanceof IntegerValue && o instanceof BigDecimalValue)
			return ((BigDecimal) o.get()).compareTo(new BigDecimal((Long) get()));
		else
			throw new VTLIncompatibleTypesException("comparison", getDomain(), o.getDomain());
	}
}
