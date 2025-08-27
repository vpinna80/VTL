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

import static it.bancaditalia.oss.vtl.util.Utils.SEQUENTIAL;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.ForkJoinTask.adapt;

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BinaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerSupplier;

/**
 * Helper class to parallelize over the two sides of a {@link BinaryTransformation}
 * 
 * @author Valentino Pinna
 *
 */
public class ThreadUtils 
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);
	
	private ThreadUtils() {}

	/**
	 * @param <T> The type returned by the subexpression evaluators
	 * 
	 * @param leftExpr function that evaluates the left subexpression
	 * @param rightExpr function that evaluates the right subexpression
	 * @param combiner a function that combines the results of the subexpressions 
	 * @return the result of the computation
	 */
	public static <T> SerFunction<Transformation, T> evalFuture(BinaryOperator<T> combiner, 
			SerFunction<? super Transformation, T> leftExpr, SerFunction<? super Transformation, T> rightExpr) 
	{
		return transformation -> {
			LOGGER.trace("Extracting subexpressions from {}", transformation);
			
			if (SEQUENTIAL)
				return combiner.apply(leftExpr.apply(transformation), rightExpr.apply(transformation));
			
			ForkJoinTask<T> left = adapt(wrap(() -> leftExpr.apply(transformation))).fork();
			ForkJoinTask<T> right = adapt(wrap(() -> rightExpr.apply(transformation))).fork();
			
			left.quietlyJoin();
			right.quietlyJoin();
			
			if (left.isCompletedNormally() && right.isCompletedNormally())
				return combiner.apply(left.getRawResult(), right.getRawResult());
			else if (!left.isCompletedNormally())
				throw new VTLNestedException("While evaluating " + transformation, left.getException());
			else
				throw new VTLNestedException("While evaluating " + transformation, right.getException());
		};
	}

	private static <T> Callable<T> wrap(SerSupplier<T> supplier)
	{
		ClassLoader loader = currentThread().getContextClassLoader();
		return () -> {
			ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
			try
			{
				Thread.currentThread().setContextClassLoader(loader);
				return supplier.get();
			}
			finally
			{
				Thread.currentThread().setContextClassLoader(oldLoader);
			}
		};
	}
}
