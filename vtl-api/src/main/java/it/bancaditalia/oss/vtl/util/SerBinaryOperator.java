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

import java.util.Comparator;
import java.util.function.BinaryOperator;

@FunctionalInterface
public interface SerBinaryOperator<T> extends BinaryOperator<T>, SerBiFunction<T, T, T>
{
    public static <T> SerBinaryOperator<T> minBy(Comparator<? super T> comparator)
    {
    	SerIntBiFunction<T, T> op = comparator::compare;
        return (a, b) -> op.apply(a, b) <= 0 ? a : b;
    }

    public static <T> SerBinaryOperator<T> maxBy(Comparator<? super T> comparator)
    {
    	SerIntBiFunction<T, T> op = comparator::compare;
        return (a, b) -> op.apply(a, b) >= 0 ? a : b;
    }
    
    public default SerBinaryOperator<T> reverseIf(boolean condition)
    {
    	return condition ? (a, b) -> apply(b, a) : this;
    }
}
