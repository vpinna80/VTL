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
import java.util.function.BiFunction;

@FunctionalInterface
public interface SerBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable
{
    public default <T1, U1> SerBiFunction<T1, U1, R> before(SerFunction<T1, T> left, SerFunction<U1, U> right)
    {
    	return new SerBiFunction<T1, U1, R>() {
			private static final long serialVersionUID = 1L;

			@Override
			public R apply(T1 t, U1 u)
			{
				return SerBiFunction.this.apply(left.apply(t), right.apply(u));
			}
		};
    }

    public default <V> SerFunction<V, R> beforeBoth(SerFunction<V, T> left, SerFunction<V, U> right)
    {
    	return new SerFunction<V, R>() {
			private static final long serialVersionUID = 1L;

			@Override
			public R apply(V v)
			{
				return SerBiFunction.this.apply(left.apply(v), right.apply(v));
			}
		};
    }
}
