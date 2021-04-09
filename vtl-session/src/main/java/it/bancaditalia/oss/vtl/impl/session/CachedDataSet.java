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
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;
import static java.util.Collections.emptySet;
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
	private static final Map<Reference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>>, Entry<String, Set<DataStructureComponent<Identifier, ?, ?>>>> REF_NAMES = new ConcurrentHashMap<>();

	private final Map<String, Semaphore> statuses;
	private final Map<Set<DataStructureComponent<Identifier, ?, ?>>, SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>>> caches;
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
	
		// the total dataset cache is a dummy index corresponding to no identifiers 
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> result = Stream.of(caches.get(keys))
				.map(SoftReference::get)
				.filter(Objects::nonNull)
				.peek(cache -> {
					isCompleted.release();
					LOGGER.trace("Cache hit for {}.", getAlias());
				}).findAny()
				// re/build the cache if gced or never created
				.orElseGet(() -> createCache(isCompleted, keys));
		
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
	
		// the total dataset cache is a dummy index corresponding to no identifiers 
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> result = Stream.of(caches.get(getComponents(Identifier.class)))
				.map(SoftReference::get)
				.filter(Objects::nonNull)
				.peek(cache -> {
					isCompleted.release();
					LOGGER.trace("Cache hit for {}.", getAlias());
				}).findAny()
				// re/build the cache if gced or never created
				.orElseGet(() -> createCache(isCompleted, getComponents(Identifier.class)));
		
		return result.values().stream().map(singletonSet -> singletonSet.iterator().next());
	}

	private Semaphore waitLock() throws InterruptedException
	{
		Semaphore isCompleted = statuses.computeIfAbsent(getAlias(), alias -> new Semaphore(1));

		while (!isCompleted.tryAcquire(1000, MILLISECONDS))
		{
			LOGGER.trace("Waiting cache completion for {}.", getAlias());
			// After 10 seconds of waiting, a deadlock is assumed 
			if (System.currentTimeMillis() - times.get(getAlias()) > 10000)
				throw stacks.get(getAlias());
		}
		
		return isCompleted;
	}

	private Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> createCache(Semaphore isCompleted, Set<DataStructureComponent<Identifier,?,?>> keys)
	{
		String alias = getAlias();
		LOGGER.debug("Cache miss for {} on {}, start caching.", alias, keys);

		// keeps track of where the deadlock may have been occurred
		IllegalStateException exception = new IllegalStateException("A deadlock may have happened in the cache mechanism. Caching for dataset " 
				+ alias + " never completed.");
		exception.getStackTrace();
		stacks.put(alias, exception);
		
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> ungroupedCache = caches.get(getComponents(Identifier.class)).get();
		if (!keys.equals(getComponents(Identifier.class)) && ungroupedCache == null)
			ungroupedCache = createCache(isCompleted, getComponents(Identifier.class));

		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> result;
		if (keys.equals(getComponents(Identifier.class)))
			try (Stream<DataPoint> stream = getDelegate().stream())
			{
				result = stream
					.map(toEntry(dp -> dp.getValues(Identifier.class), Collections::singleton))
					.collect(toConcurrentMap(Entry::getKey, Entry::getValue, (a, b) -> { 
						throw new IllegalStateException("Found duplicate datapoint: " + a.iterator().next().getValues(Identifier.class) + " and " + b.iterator().next().getValues(Identifier.class)); 
					}));
			}
		else
			result = Utils.getStream(ungroupedCache)
					.map(Entry::getValue)
					.map(Set::stream)
					.reduce(Stream::concat)
					.orElse(Stream.empty())
					.collect(groupingByConcurrent(dp -> dp.getValues(keys, Identifier.class), toSet()));
		
		SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>> cacheRef = new SoftReference<>(result, REF_QUEUE);
		caches.put(keys, cacheRef);
		REF_NAMES.put(cacheRef, new SimpleEntry<>(getAlias(), keys));
		LOGGER.debug("Caching finished for {} on {}.", alias, keys);
		isCompleted.release();
		return result;
	}
}
