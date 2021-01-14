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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;

import java.util.function.BiFunction;
import java.util.function.IntPredicate;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;

public enum ComparisonOperator implements BiFunction<ScalarValue<?, ?, ?>, ScalarValue<?, ?, ?>, ScalarValue<?, BooleanDomainSubset, BooleanDomain>>
{
	EQ("=", c -> c == 0), NE("<>", c -> c != 0), GT(">", c -> c > 0), 
	GE(">=", c -> c >= 0), LT("<", c -> c < 0), LE("<=", c -> c <= 0);
	
	private final IntPredicate lambda;
	private final String name;

	private ComparisonOperator(String name, IntPredicate lambda)
	{
		this.lambda = lambda;
		this.name = name;
	}

	@Override
	public ScalarValue<?, BooleanDomainSubset, BooleanDomain> apply(ScalarValue<?, ?, ?> l, ScalarValue<?, ?, ?> r)
	{
		return (l instanceof NullValue || r instanceof NullValue)
				? NullValue.instance(BOOLEANDS)
				: BooleanValue.of(lambda.test(l.compareTo(r)));
	}
//	
//	public BiFunction<ScalarValue<?, ?, ?>, ScalarValue<?, ?, ?>, ScalarValue<?, BooleanDomainSubset, BooleanDomain>> getAutoFunction()
//	{
//		return (l, r) -> (l instanceof NullValue || r instanceof NullValue)
//				? NullValue.instance(BOOLEANDS)
//				: l.getDomain().isAssignableFrom(r.getDomain())
//				? BooleanValue.of(lambda.test(l.compareTo(l.getDomain().cast(r)))) 
//				: BooleanValue.of(lambda.test(r.getDomain().cast(l).compareTo(r)));
//	}
	
	@Override
	public String toString()
	{
		return name;
	}
}