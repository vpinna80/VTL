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
import java.util.function.Function;

@FunctionalInterface
public interface SerFunction<T, R> extends Function<T, R>, Serializable
{
	static <T> SerFunction<T, T> identity()
	{
		return a -> a;
	}
	
	public R apply(T t);
	
	default <V> SerFunction<T, V> andThen(SerFunction<? super R, ? extends V> after)
	{
		return t -> after.apply(apply(t));
	}
	
	default <V> SerFunction<V, R> compose(SerFunction<? super V, ? extends T> before)
	{
		return v -> apply(before.apply(v));
	}
}
