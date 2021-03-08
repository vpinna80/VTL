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
package it.bancaditalia.oss.vtl.impl.session;

import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.util.Utils;

public class CachedDataSet extends NamedDataSet
{
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(CachedDataSet.class);
	private static final Map<VTLSession, Map<String, SoftReference<Set<DataPoint>>>> SESSION_CACHES = new ConcurrentHashMap<>();
	private static final Map<VTLSession, Map<String, Semaphore>> SESSION_STATUSES = new ConcurrentHashMap<>();
	private static final Map<VTLSession, Map<String, Long>> SESSION_TIMES = new ConcurrentHashMap<>();
	private static final Map<VTLSession, Map<String, IllegalStateException>> SESSION_STACKS = new ConcurrentHashMap<>();
	private static final ReferenceQueue<Set<DataPoint>> REF_QUEUE = new ReferenceQueue<>();
	private static final Map<Reference<Set<DataPoint>>, String> REF_NAMES = new ConcurrentHashMap<>();

	private final Map<String, Semaphore> statuses;
	private final Map<String, SoftReference<Set<DataPoint>>> cache;
	private final Map<String, Long> times;
	private final Map<String, IllegalStateException> stacks;
	
	static {
		new Thread() {
			
			{
				setName("CachedDataSet watcher");
				setDaemon(true);
			}
			
			@Override
			public void run() 
			{
				while (!Thread.interrupted())
					try 
					{
						String name = REF_NAMES.remove(REF_QUEUE.remove());
						if (name != null)
							LOGGER.trace("Cleaned cache of {}", name);
					}
					catch (InterruptedException e)
					{
						Thread.currentThread().interrupt();
					}
			}
			
		}.start();
	}

	public CachedDataSet(VTLSession session, String alias, DataSet delegate)
	{
		super(alias, delegate);
		
		cache = SESSION_CACHES.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
		statuses = SESSION_STATUSES.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
		times = SESSION_TIMES.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
		stacks = SESSION_STACKS.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
	}

	public CachedDataSet(VTLSession session, NamedDataSet delegate)
	{
		this(session, delegate.getAlias(), delegate.getDelegate());
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		Semaphore isCompleted = statuses.computeIfAbsent(getAlias(), alias -> new Semaphore(1));
		
		try
		{
			while (!isCompleted.tryAcquire(1000, MILLISECONDS))
			{
				LOGGER.trace("Waiting cache completion for {}.", getAlias());
				if (System.currentTimeMillis() - times.get(getAlias()) > 10000)
					throw stacks.get(getAlias());
			}
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			return Stream.empty();
		}
	
		return Stream.of(getAlias())
				.map(v -> cache.computeIfAbsent(v, alias -> new SoftReference<>(null)))
				.map(SoftReference::get)
				.filter(Objects::nonNull)
				.map(Utils::getStream)
				.peek(cache -> {
					isCompleted.release();
					LOGGER.trace("Cache hit for {}.", getAlias());
				}).findAny()
				.orElseGet(() -> createCache(isCompleted));
	}

	private Stream<DataPoint> createCache(Semaphore isCompleted)
	{
		AtomicBoolean alreadyInterrupted = new AtomicBoolean(false);
		LOGGER.debug("Cache miss for {}, start caching.", getAlias());
		
		// reference to cache holder 
		SoftReference<Set<DataPoint>> setRef = new SoftReference<>(newSetFromMap(new ConcurrentHashMap<>()), REF_QUEUE);
		IllegalStateException exception = new IllegalStateException("A deadlock may have happened in the cache mechanism. Caching for dataset " 
				+ getAlias() + " never completed.");
		exception.getStackTrace();
		stacks.put(getAlias(), exception);
		return getDelegate().stream()
			.peek(dp -> {
				// enqueue datapoint if reference was not gced
				Set<DataPoint> set = setRef.get();
				if (set != null)
				{
					LOGGER.trace("Caching a datapoint for {}.", getAlias());
					times.merge(getAlias(), System.currentTimeMillis(), Math::max);
					set.add(dp);
				}
				else if (!alreadyInterrupted.get() && alreadyInterrupted.compareAndSet(false, true))
				{
					LOGGER.trace("Caching interrupted for {}.", getAlias());
					isCompleted.release();
				}
			}).onClose(() -> {
				if (!alreadyInterrupted.get() && alreadyInterrupted.compareAndSet(false, true))
				{
					LOGGER.debug("Caching finished for {}.", getAlias());
					// register the cache
					cache.put(getAlias(), setRef);
					// remember the name
					REF_NAMES.put(setRef, getAlias());
				}
				isCompleted.release();
			});
	}
}
