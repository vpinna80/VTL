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
package it.bancaditalia.oss.vtl.impl.transform.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.bool.IsNullTransformation;
import it.bancaditalia.oss.vtl.impl.transform.time.CurrentDateOperand;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.Utils;

/**
 * Helper class to parallelize over the two sides of a {@link BinaryTransformation}
 * 
 * @author Valentino Pinna
 *
 */
public class ThreadUtils 
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);
	private final static Set<Class<? extends Transformation>> SIMPLE_TRANSFORMATIONS = new HashSet<>();
	
	static {
		SIMPLE_TRANSFORMATIONS.add(ConstantOperand.class);
		SIMPLE_TRANSFORMATIONS.add(VarIDOperand.class);
		SIMPLE_TRANSFORMATIONS.add(CurrentDateOperand.class);
		SIMPLE_TRANSFORMATIONS.add(IsNullTransformation.class);
	}
	
	private ThreadUtils() {}

	/**
	 * @param <T> The type returned by the extractor
	 * 
	 * @param extractors functions that extracts the results of each subexpression
	 * @param combiner an associative function that reduces the results of the subexpressions 
	 * @return the result of the computation
	 */
	@SafeVarargs
	public static <T> Function<Transformation, T> evalFuture(BinaryOperator<T> combiner, Function<? super Transformation, T>... extractors) 
	{
		return transformation -> {
			LOGGER.debug("Extracting {} subexpressions from {}", extractors.length, transformation);
			Stream<Function<? super Transformation, T>> stream = Arrays.stream(extractors);
			if (Utils.SEQUENTIAL)
				LOGGER.trace("Computation is done sequentially");
			else
			{
				LOGGER.trace("Computation is done concurrently");
				stream = stream.parallel();
			}
			T result = stream.map(e -> e.apply(transformation)).reduce(combiner).get();
			LOGGER.debug("Reduced result for {}", transformation);
			return result;
		};
	}
}
