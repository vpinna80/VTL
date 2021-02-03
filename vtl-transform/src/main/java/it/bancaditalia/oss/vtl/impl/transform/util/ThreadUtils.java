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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class ThreadUtils 
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);
	private final static ExecutorService POOL = Executors.newCachedThreadPool(r -> {
		Thread thread = new Thread(r);
		thread.setDaemon(true);
		return thread;
	}); 
	
	private ThreadUtils() {}

	public static <T> T evalFuture(boolean isMeta, TransformationScheme scheme, BinaryTransformation reducingExpr, BiFunction<? super T, ? super T, ? extends T> finisher, 
			BiFunction<? super Transformation, ? super TransformationScheme, ? extends T> extractor, Transformation leftExpr, Transformation rightExpr) 
	{
		if (Utils.SEQUENTIAL)
			return finisher.apply(extractor.apply(leftExpr, scheme), extractor.apply(rightExpr, scheme));

		final String what = isMeta ? "metadata" : "value";
	
		LOGGER.trace("Asking computing {} of {}:{}", what, reducingExpr.getClass().getSimpleName(), reducingExpr);
		Future<? extends T> leftTask = POOL.submit(() -> extractor.apply(leftExpr, scheme));
		Future<? extends T> rightTask = POOL.submit(() -> extractor.apply(rightExpr, scheme));

		T left = null, right = null;
		try 
		{
			try
			{
				left = leftTask.get(500, MILLISECONDS);
			}
			catch (TimeoutException e)
			{
				LOGGER.trace("Thread starvation querying for {}:{}", reducingExpr.getLeftOperand().getClass().getSimpleName(), reducingExpr.getLeftOperand());
				left = extractor.apply(leftExpr, scheme);
			}

			try
			{
				right = rightTask.get(500, MILLISECONDS);
			}
			catch (TimeoutException e)
			{
				LOGGER.trace("Thread starvation querying for {}:{}", reducingExpr.getRightOperand().getClass().getSimpleName(), reducingExpr.getRightOperand());
				right = extractor.apply(rightExpr, scheme);
			}
		}
		catch (InterruptedException | ExecutionException e) 
		{
			throw new VTLNestedException("Error executing subrule " + leftExpr, e);
		}

		return finisher.apply(left, right);
	}
}
