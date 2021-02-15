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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.bool.IsNullTransformation;
import it.bancaditalia.oss.vtl.impl.transform.time.CurrentDateOperand;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
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
	private final static ForkJoinPool POOL;
	private final static Set<Class<? extends Transformation>> SIMPLE_TRANSFORMATIONS = new HashSet<>();
	
	static {
		// Compensate for few cores available and avoid starvation
		final int parallelism = ForkJoinPool.commonPool().getParallelism();
		if (parallelism < 32)
			POOL = new ForkJoinPool(32 - parallelism);
		else
			POOL = ForkJoinPool.commonPool();
		
		SIMPLE_TRANSFORMATIONS.add(ConstantOperand.class);
		SIMPLE_TRANSFORMATIONS.add(VarIDOperand.class);
		SIMPLE_TRANSFORMATIONS.add(CurrentDateOperand.class);
		SIMPLE_TRANSFORMATIONS.add(IsNullTransformation.class);
	}
	
	private ThreadUtils() {}

	/**
	 * @param <T> The type returned by the extractor
	 * 
	 * @param scheme The {@link TransformationScheme}
	 * @param expr The binary expression being computed
	 * @param extractor a function that extracts the value from the result of each subexpression
	 * @param combiner a function that combines the results of the two subexpressions 
	 * @return
	 */
	public static <T> T evalFuture(TransformationScheme scheme, BinaryTransformation expr,
			BiFunction<? super T, ? super T, ? extends T> combiner, 
			BiFunction<? super Transformation, ? super TransformationScheme, ? extends T> extractor) 
	{
		Transformation leftExpr = expr.getLeftOperand();
		Transformation rightExpr = expr.getRightOperand();
		
		if (Utils.SEQUENTIAL)
			return combiner.apply(extractor.apply(leftExpr, scheme), extractor.apply(rightExpr, scheme));

		LOGGER.trace("Asking computation of {}:{}", expr.getClass().getSimpleName(), expr);
		
		ForkJoinTask<? extends T> leftTask = null, rightTask = null;
		if (!SIMPLE_TRANSFORMATIONS.contains(leftExpr.getClass()))
			leftTask = POOL.submit(() -> extractor.apply(leftExpr, scheme));
		if (!SIMPLE_TRANSFORMATIONS.contains(rightExpr.getClass()))
			rightTask = POOL.submit(() -> extractor.apply(rightExpr, scheme));
		
		T left = leftTask != null ? leftTask.join() : extractor.apply(leftExpr, scheme); 
		T right = rightTask != null ? rightTask.join() : extractor.apply(rightExpr, scheme); 

		return combiner.apply(left, right);
	}
}
