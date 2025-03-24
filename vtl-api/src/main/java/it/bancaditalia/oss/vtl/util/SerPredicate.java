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
package it.bancaditalia.oss.vtl.util;

import java.io.Serializable;
import java.util.function.Predicate;

@FunctionalInterface
public interface SerPredicate<T> extends Predicate<T>, Serializable
{
	public default <U> SerPredicate<U> compose(SerFunction<U, T> function)
	{
		return u -> test(function.apply(u));
	}
	
	public static <T> SerPredicate<T> not(SerPredicate<T> predicate)
	{
		return predicate.negate();
	}
	
	@Override
	public default SerPredicate<T> negate()
	{
		return a -> !test(a);
	}
	
	@Override
	public default SerPredicate<T> and(Predicate<? super T> other)
	{
		return (t) -> test(t) && other.test(t);
	}
	
	@Override
	public default SerPredicate<T> or(Predicate<? super T> other)
	{
		return (t) -> test(t) || other.test(t);
	}
}
