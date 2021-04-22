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

import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toSet;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.util.Utils;

public class CachedDataSet extends NamedDataSet
{
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(CachedDataSet.class);
	private static final Map<VTLSession, Map<String, Semaphore>> SESSION_STATUSES = new ConcurrentHashMap<>();
	private static final Map<VTLSession, Map<String, Long>> SESSION_TIMES = new ConcurrentHashMap<>();
	private static final Map<VTLSession, Map<String, IllegalStateException>> SESSION_STACKS = new ConcurrentHashMap<>();
	private static final Map<VTLSession, Map<String, Map<Set<DataStructureComponent<Identifier, ?, ?>>, SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>>>>> SESSION_CACHES = new ConcurrentHashMap<>();
	private static final ReferenceQueue<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>> REF_QUEUE = new ReferenceQueue<>();
	private static final Map<Reference<?>, Entry<String, Set<DataStructureComponent<Identifier, ?, ?>>>> REF_NAMES = new ConcurrentHashMap<>();

	private transient final Map<String, Semaphore> statuses;
	private transient final Map<Set<DataStructureComponent<Identifier, ?, ?>>, SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>>> caches;
	private transient final Map<String, Long> times;
	private transient final Map<String, IllegalStateException> stacks;
	
	private transient volatile SoftReference<Set<DataPoint>> unindexed = new SoftReference<>(null);
	
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
						Entry<String, Set<DataStructureComponent<Identifier, ?, ?>>> data = REF_NAMES.remove(REF_QUEUE.remove());
						if (data != null)
							LOGGER.trace("Cleaned an index of {} over {}", data.getKey(), data.getValue());
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
		
		caches = SESSION_CACHES.computeIfAbsent(session, s -> new ConcurrentHashMap<>()).computeIfAbsent(alias, a -> initCache());
		statuses = SESSION_STATUSES.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
		times = SESSION_TIMES.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
		stacks = SESSION_STACKS.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
	}

	private Map<Set<DataStructureComponent<Identifier, ?, ?>>, SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>>> initCache()
	{
		final Map<Set<DataStructureComponent<Identifier, ?, ?>>, SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>>> datasetCache = new HashMap<>();

		List<Set<DataStructureComponent<Identifier, ?, ?>>> accumulator = new ArrayList<>();
		accumulator.add(emptySet());
		// build all ids subsets
		for (DataStructureComponent<Identifier, ?, ?> key: getDelegate().getComponents(Identifier.class))
			accumulator = accumulator.stream()
				.flatMap(set -> {
					Set<DataStructureComponent<Identifier, ?, ?>> newSet = new HashSet<>(set);
					newSet.add(key);
					return Stream.of(set, newSet);
				})
				.collect(toCollection(ArrayList::new));
		for (Set<DataStructureComponent<Identifier, ?, ?>> keySet: accumulator)
			datasetCache.put(keySet, new SoftReference<>(null));
			
		return datasetCache;
	}

	public CachedDataSet(VTLSession session, NamedDataSet delegate)
	{
		this(session, delegate.getAlias(), delegate.getDelegate());
	}

	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter, Collector<DataPoint, A, TT> groupCollector,
			BiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		// the total dataset cache is a dummy index corresponding to no identifiers 
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> result = Stream.of(caches.get(keys))
				.map(SoftReference::get)
				.filter(Objects::nonNull)
				.peek(cache -> LOGGER.trace("Cache hit for {}.", getAlias()))
				.findAny()
				// re/build the cache if gced or never created
				.orElseGet(() -> createCache(keys));
		
		Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filterOutsideKeys = new HashMap<>(filter);
		filterOutsideKeys.keySet().retainAll(keys);
		
		return Utils.getStream(result)
			.filter(entryByKey(idVals -> idVals.entrySet().containsAll(filter.entrySet())))
			.map(keepingKey(Set::stream))
			.map(keepingKey(s -> s.filter(dp -> filterOutsideKeys.isEmpty() || dp.matches(filter))))
			.map(keepingKey(s -> s.collect(groupCollector)))
			.map(splitting((k, v) -> finisher.apply(v,  k)));
	}
	
	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		Semaphore isCompleted;
		try
		{
			isCompleted = waitLock();
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			return Stream.empty();
		}

		Set<DataPoint> cache = unindexed.get();
		if (cache != null)
		{
			isCompleted.release();
			return cache.stream();
		}
		else
			return createUnindexedCache(isCompleted);
	}

	private Semaphore waitLock() throws InterruptedException
	{
		final String alias = getAlias();
		LOGGER.trace("Acquiring lock for {}", alias);
		
		Semaphore isCompleted = statuses.computeIfAbsent(alias, a -> new Semaphore(1, true));

		while (!isCompleted.tryAcquire(1000, MILLISECONDS))
		{
			LOGGER.trace("Waiting cache completion for {}.", alias);
			// After 10 seconds of waiting, a deadlock is assumed 
			if (times.containsKey(alias) && System.currentTimeMillis() - times.get(alias) > 10000)
				throw stacks.get(alias);
		}
		
		LOGGER.trace("Lock acquired for {}", alias);
		return isCompleted;
	}

	private Stream<DataPoint> createUnindexedCache(Semaphore isCompleted)
	{
		String alias = getAlias();

		LOGGER.debug("Cache miss for {}, start caching.", alias);
		Set<DataPoint> cache = newSetFromMap(new ConcurrentHashMap<>());
		unindexed = new SoftReference<Set<DataPoint>>(cache);
		
		IllegalStateException exception = new IllegalStateException("A deadlock may have happened in the cache mechanism. Caching for dataset " 
				+ alias + " never completed.");
		exception.getStackTrace();
		stacks.put(alias, exception);
		times.merge(alias, System.currentTimeMillis(), Math::max);
		
		AtomicBoolean alreadyInterrupted = new AtomicBoolean(false);
		return getDelegate().stream()
			.peek(dp -> {
				// enqueue datapoint if reference was not gced
				Set<DataPoint> set = unindexed.get();
				if (set != null)
				{
					LOGGER.trace("Caching a datapoint for {}.", alias);
					times.merge(alias, System.currentTimeMillis(), Math::max);
					set.add(dp);
				}
				else if (!alreadyInterrupted.get() && alreadyInterrupted.compareAndSet(false, true))
				{
					LOGGER.trace("Caching interrupted for {}.", alias);
					isCompleted.release();
				}
			}).onClose(() -> {
				if (!alreadyInterrupted.get() && alreadyInterrupted.compareAndSet(false, true))
					LOGGER.debug("Caching finished for {}.", alias);
				isCompleted.release();
			});
	}

	private Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> createCache(Set<DataStructureComponent<Identifier,?,?>> keys)
	{
		String alias = getAlias();
		LOGGER.debug("Cache miss for {}, start indexing on {}.", alias, keys);

		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> result;
		Set<DataPoint> unindexedCache = unindexed.get();
		if (unindexedCache == null)
		{
			Semaphore isCompleted;
			try
			{
				isCompleted = waitLock();
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				return emptyMap();
			}

			unindexedCache = createUnindexedCache(isCompleted).collect(toSet());
			unindexed = new SoftReference<>(unindexedCache);
		}

		if (getComponents(Identifier.class).equals(keys))
			result = Utils.getStream(unindexedCache)
				.collect(toConcurrentMap(dp -> dp.getValues(keys, Identifier.class), Collections::singleton));
		else
			result = Utils.getStream(unindexedCache)
				.collect(groupingByConcurrent(dp -> dp.getValues(keys, Identifier.class), toSet()));
		
		SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>> cacheRef = new SoftReference<>(result, REF_QUEUE);
		caches.put(keys, cacheRef);
		REF_NAMES.put(cacheRef, new SimpleEntry<>(getAlias(), keys));
		LOGGER.debug("Indexing finished for {} on {}.", alias, keys);
		return result;
	}
}
