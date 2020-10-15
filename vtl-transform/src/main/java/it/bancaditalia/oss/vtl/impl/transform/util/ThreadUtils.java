package it.bancaditalia.oss.vtl.impl.transform.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
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

	public static <T> T evalFuture(boolean isMeta, TransformationScheme scheme, Transformation reducingExpr, BiFunction<? super T, ? super T, ? extends T> finisher, 
			BiFunction<? super Transformation, ? super TransformationScheme, ? extends T> extractor, Transformation leftExpr, Transformation rightExpr) 
	{
		if (Utils.SEQUENTIAL)
			return finisher.apply(extractor.apply(leftExpr, scheme), extractor.apply(rightExpr, scheme));

		final String what = isMeta ? "metadata" : "value";
	
		LOGGER.trace("Asking computing {} of {}:{}", what, hashHex(reducingExpr), reducingExpr);
		Future<? extends T> leftTask = POOL.submit(() -> extractor.apply(leftExpr, scheme));
		Future<? extends T> rightTask = POOL.submit(() -> extractor.apply(rightExpr, scheme));

		final T left, right;
		try 
		{
			left = leftTask.get();
		}
		catch (InterruptedException | ExecutionException e) 
		{
			throw new VTLNestedException("Error executing subrule " + leftExpr, e);
		}
		
		try 
		{
			right = rightTask.get();
		}
		catch (InterruptedException e) 
		{
			throw new VTLNestedException("Error executing subrule " + rightExpr, e);
		}
		catch (ExecutionException e) 
		{
			throw new VTLNestedException("Error executing subrule " + rightExpr, e.getCause());
		}

		return finisher.apply(left, right);
	}

	private static String hashHex(Transformation expr) 
	{
		return String.format("%08X", expr.hashCode());
	}
}
