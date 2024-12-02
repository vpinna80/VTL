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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static java.lang.Double.NaN;
import static java.lang.Math.log;
import static java.lang.Math.pow;
import static java.math.MathContext.DECIMAL128;

import java.math.BigDecimal;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongBinaryOperator;

import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;

public enum ArithmeticOperator
{
	SUM(" + ", Double::sum, BigDecimal::add, Long::sum),
	DIFF(" - ", (l, r) -> l - r, BigDecimal::subtract, (l, r) -> l - r), 
	MULT(" * ", (l, r) -> l * r, (l, r) -> l.multiply(r, DECIMAL128), (l, r) -> l * r), 
	DIV(" / ", (l, r) -> l / r, (l, r) -> l.divide(r, DECIMAL128), (l, r) -> l / r),
	MOD("mod", (l, r) -> l % r, BigDecimal::remainder, (l, r) -> l % r),
	POWER("power", Math::pow, (l, r) -> BigDecimal.valueOf(pow(l.doubleValue(), r.doubleValue())), (l, r) -> longPower(l, r)),
	LOG("log", (l, r) -> log(l) / log(r), (l, r) -> BigDecimal.valueOf(log(l.doubleValue()) / log(r.doubleValue())), (l, r) -> (long) (log(l) / log(r)));

	private final String name;
	private final LongBinaryOperator opLong;
	private final DoubleBinaryOperator opDouble;
	private final SerBinaryOperator<BigDecimal> opBigd;
	
	private ArithmeticOperator(String name, DoubleBinaryOperator opDouble, SerBinaryOperator<BigDecimal> opBigd, LongBinaryOperator opLong)
	{
		this.name = name;
		this.opLong = opLong;
		this.opDouble = opDouble;
		this.opBigd = opBigd;
	}

	public ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> applyAsInteger(ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return NullValue.instance(INTEGERDS);
		return IntegerValue.of(opLong.applyAsLong(((Number) (INTEGERDS.cast(left)).get()).longValue(), 
				(((Number) INTEGERDS.cast(right).get()).longValue())));
	}
	
	public ScalarValue<?, ?, ?, ?> applyAsNumber(ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return NullValue.instance(NUMBERDS);
		else if (isUseBigDecimal()) 
			return BigDecimalValue.of(opBigd.apply(toBigDecimal(((Number) (NUMBERDS.cast(left)).get())), 
					toBigDecimal((((Number) NUMBERDS.cast(right).get()).doubleValue()))));
		else
		{
			double leftD = ((Number) (NUMBERDS.cast(left)).get()).doubleValue();
			double rightD = ((Number) (NUMBERDS.cast(right)).get()).doubleValue();
			
			if (!Double.isFinite(leftD) || !Double.isFinite(rightD))
				return NumberValueImpl.createNumberValue(NaN);
			
			return NumberValueImpl.createNumberValue(opDouble.applyAsDouble(leftD, rightD));
		}
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
	
	private static long longPower(long l, long r)
	{
		double pow = Math.pow(l, r);
		if (!Double.isFinite(pow) || pow >= (double) Long.MAX_VALUE || pow <= (double) Long.MIN_VALUE)
			return Long.MIN_VALUE;
		else
			return (long) pow;
	}
}