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
package it.bancaditalia.oss.vtl.impl.types.operators;

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static java.math.RoundingMode.DOWN;
import static java.math.RoundingMode.HALF_UP;

import java.math.BigDecimal;

import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBigDecimalIntBiFunction;
import it.bancaditalia.oss.vtl.util.SerDoubleIntBiFunction;

public enum NumericIntOperator implements SerBiFunction<
		ScalarValue<?, ?, ?, ?>, 
		ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain>,
		ScalarValue<?, ?, ?, ?>>
{
	ROUND("round", (l, r) -> BigDecimal.valueOf(l).setScale((int) r, HALF_UP).doubleValue(), (l, r) -> l.setScale(r, HALF_UP)),
	TRUNC("trunc", (l, r) -> BigDecimal.valueOf(l).setScale((int) r, DOWN).doubleValue(), (l, r) -> l.setScale(r, DOWN)),
	RANDOM("random", (l, r) -> { throw new UnsupportedOperationException("random"); }, (l, r) -> { throw new UnsupportedOperationException("random"); });

	private final String name;
	private final SerDoubleIntBiFunction opDouble;
	private final SerBigDecimalIntBiFunction opBigDec;

	private NumericIntOperator(String name, SerDoubleIntBiFunction opDouble, SerBigDecimalIntBiFunction opBigDec)
	{
		this.name = name;
		this.opDouble = opDouble;
		this.opBigDec = opBigDec;
	}

	@Override
	public ScalarValue<?, ?, ?, ?> apply(ScalarValue<?, ?, ?, ?> t, 
			ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> u)
	{
		if (t.getClass() == NullValue.class)
			return t;

		Number result;
		if (isUseBigDecimal())
			result = opBigDec.apply((BigDecimal) t.get(), ((Number) u.get()).intValue());
		else
			result = opDouble.applyAsDouble(((Number) t.get()).doubleValue(), ((Number) u.get()).intValue());

		return t.getDomain() instanceof IntegerDomain ? IntegerValue.of(result.longValue()) : createNumberValue(result);
	}
	
	@Override
	public String toString()
	{
		return name;
	}
}
