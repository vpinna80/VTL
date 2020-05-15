/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.types.operators;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static java.lang.Math.log;
import static java.lang.Math.pow;
import static java.math.RoundingMode.DOWN;
import static java.math.RoundingMode.HALF_UP;

import java.math.BigDecimal;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongBinaryOperator;

import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;

public enum ArithmeticOperator
{
	SUM(" + ", (l, r) -> l + r, (l, r) -> l + r),
	DIFF(" - ", (l, r) -> l - r, (l, r) -> l - r), 
	MULT(" * ", (l, r) -> l * r, (l, r) -> l * r), 
	DIV(" / ", (l, r) -> l / r, (l, r) -> l / r),
	MOD(" mod ", (l, r) -> l % r, (l, r) -> l % r),
	POWER(" power ", Math::pow, (l, r) -> (long) pow(l, r)),
	LOG(" log ", (l, r) -> log(l) / log(r), (l, r) -> (long) (log(l) / log(r))),
	ROUND(" round ", (l, r) -> BigDecimal.valueOf(l).setScale((int) r, HALF_UP).doubleValue(), (l, r) -> BigDecimal.valueOf(l).setScale((int) r, HALF_UP).intValue()),
	TRUNC(" trunc ", (l, r) -> BigDecimal.valueOf(l).setScale((int) r, DOWN).doubleValue(), (l, r) -> BigDecimal.valueOf(l).setScale((int) r, DOWN).intValue());

	private final DoubleBinaryOperator opDouble;
	private final LongBinaryOperator opInteger;
	private final String name;
	
	private ArithmeticOperator(String name, DoubleBinaryOperator opDouble, LongBinaryOperator opInteger)
	{
		this.name = name;
		this.opDouble = opDouble;
		this.opInteger = opInteger;
	}

	public ScalarValue<?, NumberDomainSubset<NumberDomain>, NumberDomain> applyAsDouble(ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return NullValue.instance(NUMBERDS);
		return new DoubleValue(opDouble.applyAsDouble(((Number) (NUMBERDS.cast(left)).get()).doubleValue(), 
				(((Number) NUMBERDS.cast(right).get()).doubleValue())));
	}

	public ScalarValue<?, IntegerDomainSubset, IntegerDomain> applyAsInt(ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return NullValue.instance(INTEGERDS);
		return new IntegerValue(opInteger.applyAsLong(((Number) (INTEGERDS.cast(left)).get()).longValue(), 
				(((Number) INTEGERDS.cast(right).get()).longValue())));
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