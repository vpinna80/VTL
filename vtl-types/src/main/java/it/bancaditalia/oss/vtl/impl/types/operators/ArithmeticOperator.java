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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.USE_BIG_DECIMAL;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static java.lang.Math.log;
import static java.lang.Math.pow;
import static java.math.MathContext.DECIMAL128;
import static java.math.RoundingMode.DOWN;
import static java.math.RoundingMode.HALF_UP;

import java.math.BigDecimal;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongBinaryOperator;

import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;

public enum ArithmeticOperator
{
	SUM(" + ", (l, r) -> l + r, (l, r) -> l + r, BigDecimal::add),
	DIFF(" - ", (l, r) -> l - r, (l, r) -> l - r, BigDecimal::subtract), 
	MULT(" * ", (l, r) -> l * r, (l, r) -> l * r, (l, r) -> l.multiply(r, DECIMAL128)), 
	DIV(" / ", (l, r) -> l / r, (l, r) -> l / r, (l, r) -> l.divide(r, DECIMAL128)),
	MOD("mod", (l, r) -> l % r, (l, r) -> l % r, BigDecimal::remainder),
	POWER("power", (l, r) -> (long) pow(l, r), Math::pow, (l, r) -> BigDecimal.valueOf(pow(l.doubleValue(), r.doubleValue()))),
	LOG("log", (l, r) -> (long) (log(l) / log(r)), (l, r) -> log(l) / log(r), (l, r) -> BigDecimal.valueOf(log(l.doubleValue()) / log(r.doubleValue()))),
	ROUND("round", (l, r) -> BigDecimal.valueOf(l).setScale((int) r, HALF_UP).intValue(), (l, r) -> BigDecimal.valueOf(l).setScale((int) r, HALF_UP).doubleValue(), (l, r) -> l.setScale(r.intValue(), HALF_UP)),
	TRUNC("trunc", (l, r) -> BigDecimal.valueOf(l).setScale((int) r, DOWN).intValue(), (l, r) -> BigDecimal.valueOf(l).setScale((int) r, DOWN).doubleValue(), (l, r) -> l.setScale(r.intValue(), DOWN));

	private final String name;
	private final LongBinaryOperator opLong;
	private final DoubleBinaryOperator opDouble;
	private final SerBinaryOperator<BigDecimal> opBigd;
	
	private ArithmeticOperator(String name, LongBinaryOperator opLong, DoubleBinaryOperator opDouble, SerBinaryOperator<BigDecimal> opBigd)
	{
		this.name = name;
		this.opLong = opLong;
		this.opDouble = opDouble;
		this.opBigd = opBigd;
	}

	public ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> applyAsInt(ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return NullValue.instance(INTEGERDS);
		return IntegerValue.of(opLong.applyAsLong(((Number) (INTEGERDS.cast(left)).get()).longValue(), 
				(((Number) INTEGERDS.cast(right).get()).longValue())));
	}
	
	public ScalarValue<?, ?, ? extends NumberDomainSubset<?, ? extends NumberDomain>, ? extends NumberDomain> applyAsNumber(ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (Boolean.valueOf(USE_BIG_DECIMAL.getValue())) 
			if (left instanceof NullValue || right instanceof NullValue)
				return NullValue.instance(NUMBERDS);
			else
				return BigDecimalValue.of(opBigd.apply(toBigDecimal(((Number) (NUMBERDS.cast(left)).get())), 
						toBigDecimal((((Number) NUMBERDS.cast(right).get()).doubleValue()))));
		else if (left instanceof NullValue || right instanceof NullValue)
				return NullValue.instance(NUMBERDS);
			else
				return DoubleValue.of(opDouble.applyAsDouble(((Number) (NUMBERDS.cast(left)).get()).doubleValue(), 
						(((Number) NUMBERDS.cast(right).get()).doubleValue())));
	}

	private static BigDecimal toBigDecimal(Number number)
	{
		if (number instanceof BigDecimal)
			return (BigDecimal) number;
		else if (number instanceof Double)
			return BigDecimal.valueOf(number.doubleValue());
		else if (number instanceof Long)
			return BigDecimal.valueOf(number.longValue());
		else
			throw new IllegalStateException();
	}

	@Override
	public String toString()
	{
		return name;
	}

	public boolean isInfix()
	{
		return this == SUM || this == DIFF || this == MULT || this == DIV;
	}
}