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
import static java.util.Collections.emptySet;
import static java.util.Collections.newSetFromMap;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.util.Utils;

public class CachedDataSet extends NamedDataSet
{
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(CachedDataSet.class);
	private static final Map<VTLSession, Map<String, IllegalStateException>> SESSION_STACKS = new ConcurrentHashMap<>();
	private static final Map<VTLSession, Map<String, CacheWaiter>> SESSION_CACHES = new ConcurrentHashMap<>();
	private static final ReferenceQueue<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>> REF_QUEUE = new ReferenceQueue<>();
	private static final Map<Reference<?>, Entry<String, Set<DataStructureComponent<Identifier, ?, ?>>>> REF_NAMES = new ConcurrentHashMap<>();

	private final Map<String, IllegalStateException> stacks;
	private final transient CacheWaiter waiter;
	private transient volatile SoftReference<Set<DataPoint>> unindexed = new SoftReference<>(null);
	
	private static class CacheWaiter implements ManagedBlocker
	{
		private final Semaphore semaphore = new Semaphore(1);
		private final Map<Set<DataStructureComponent<Identifier, ?, ?>>, SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>>> cache = new ConcurrentHashMap<>();
		private final String alias;

		public CacheWaiter(CachedDataSet delegate)
		{
			alias = delegate.getAlias();
			
			List<Set<DataStructureComponent<Identifier, ?, ?>>> accumulator = new ArrayList<>();
			accumulator.add(emptySet());
			// build all ids subsets
			for (DataStructureComponent<Identifier, ?, ?> key: delegate.getComponents(Identifier.class))
				accumulator = accumulator.stream()
					.flatMap(set -> {
						Set<DataStructureComponent<Identifier, ?, ?>> newSet = new HashSet<>(set);
						newSet.add(key);
						return Stream.of(set, newSet);
					})
					.collect(toCollection(ArrayList::new));
			for (Set<DataStructureComponent<Identifier, ?, ?>> keySet: accumulator)
				cache.put(keySet, new SoftReference<>(null));
		}
		
		@Override
		public boolean block() throws InterruptedException
		{
			semaphore.acquire();
			return true;
		}

		@Override
		public boolean isReleasable()
		{
			return semaphore.tryAcquire();
		}
		
		public void putCache(Set<DataStructureComponent<Identifier, ?, ?>> keys, Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> newCache)
		{
			SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>> cacheRef = new SoftReference<>(newCache, REF_QUEUE);
			REF_NAMES.put(cacheRef, new SimpleEntry<>(alias, keys));
			cache.put(keys, new SoftReference<>(newCache));
		}
		
		public Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> getCache(Set<DataStructureComponent<Identifier, ?, ?>> keys)
		{
			return cache.get(keys).get();
		}
		
		public void done()
		{
			semaphore.release();
		}
	}
	
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

	public CachedDataSet(VTLSessionImpl session, String alias, DataSet delegate)
	{
		super(alias, delegate);
		
		waiter = SESSION_CACHES.computeIfAbsent(session, s -> new ConcurrentHashMap<>()).computeIfAbsent(alias, a -> new CacheWaiter(this));
		stacks = SESSION_STACKS.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
	}

	public CachedDataSet(VTLSessionImpl session, NamedDataSet delegate)
	{
		this(session, delegate.getAlias(), delegate.getDelegate());
	}

	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter, Collector<DataPoint, A, TT> groupCollector,
			BiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		try
		{
			ForkJoinPool.managedBlock(waiter);
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			return Stream.empty();
		}
		
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> value = waiter.getCache(keys);
		if (value == null)
			value = createCache(keys);
		else
		{
			LOGGER.trace("Cache hit for {}.", getAlias());
			waiter.done();
		}
			
		Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filterOutsideKeys = new HashMap<>(filter);
		filterOutsideKeys.keySet().retainAll(keys);
		
		return Utils.getStream(value)
			.filter(entryByKey(idVals -> idVals.entrySet().containsAll(filter.entrySet())))
			.map(keepingKey(Set::stream))
			.map(keepingKey(s -> s.filter(dp -> filterOutsideKeys.isEmpty() || dp.matches(filter))))
			.map(keepingKey(s -> s.collect(groupCollector)))
			.map(splitting((k, v) -> finisher.apply(v,  k)));
	}
	
	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		try
		{
			ForkJoinPool.managedBlock(waiter);
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			return Stream.empty();
		}

		Set<DataPoint> cache = unindexed.get();
		if (cache != null)
		{
			waiter.done();
			return cache.stream();
		}
		else
			return createUnindexedCache(true);
	}

	@Override
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, BiPredicate<DataPoint, DataPoint> predicate, BinaryOperator<DataPoint> mergeOp)
	{
		try
		{
			ForkJoinPool.managedBlock(waiter);
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			return new LightDataSet(metadata, Stream::empty);
		}
	
		Set<DataStructureComponent<Identifier, ?, ?>> commonIds = getMetadata().getComponents(Identifier.class);
		commonIds.retainAll(other.getComponents(Identifier.class));
		
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> value = waiter.getCache(commonIds);
		if (value == null)
			value = createCache(commonIds);
		else
		{
			LOGGER.trace("Cache hit for {}.", getAlias());
			waiter.done();
		}

		BiPredicate<DataPoint, DataPoint> newPredicate = (a, b) -> predicate.test(b, a);
		BinaryOperator<DataPoint> newMergeOp = (a, b) -> mergeOp.apply(b, a);
		return filteredMappedJoinWithIndex(other, metadata, newPredicate, newMergeOp, commonIds, value);
	}

	protected Stream<DataPoint> createUnindexedCache(boolean unlockWhenComplete)
	{
		String alias = getAlias();

		LOGGER.debug("Cache miss for {}, start caching.", alias);
		Set<DataPoint> cache = newSetFromMap(new ConcurrentHashMap<>());
		unindexed = new SoftReference<Set<DataPoint>>(cache);
		
		IllegalStateException exception = new IllegalStateException("A deadlock may have happened in the cache mechanism. Caching for dataset " 
				+ alias + " never completed.");
		exception.getStackTrace();
		stacks.put(alias, exception);
		
		AtomicBoolean alreadyInterrupted = new AtomicBoolean(false);
		return getDelegate().stream()
			.peek(dp -> {
				// enqueue datapoint if reference was not gced
				Set<DataPoint> set = unindexed.get();
				if (set != null)
				{
					LOGGER.trace("Caching a datapoint for {}.", alias);
					set.add(dp);
				}
				else if (!alreadyInterrupted.get() && alreadyInterrupted.compareAndSet(false, true))
				{
					LOGGER.trace("Caching interrupted for {}.", alias);
					if (unlockWhenComplete)
						waiter.done();
				}
			}).onClose(() -> {
				if (!alreadyInterrupted.get() && alreadyInterrupted.compareAndSet(false, true))
					LOGGER.debug("Caching finished for {}.", alias);
				if (unlockWhenComplete)
					waiter.done();
			});
	}

	protected Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> createCache(Set<DataStructureComponent<Identifier,?,?>> keys)
	{
		String alias = getAlias();
		LOGGER.debug("Cache miss for {}, start indexing on {}.", alias, keys);

		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> result;
		Set<DataPoint> unindexedCache = unindexed.get();
		if (unindexedCache == null)
			try (Stream<DataPoint> stream = createUnindexedCache(false))
			{
				unindexedCache = stream.collect(toSet());
				unindexed = new SoftReference<>(unindexedCache);
			}

		if (getComponents(Identifier.class).equals(keys))
			result = Utils.getStream(unindexedCache)
				.collect(toConcurrentMap(dp -> dp.getValues(keys, Identifier.class), Collections::singleton));
		else
			result = Utils.getStream(unindexedCache)
				.collect(groupingByConcurrent(dp -> dp.getValues(keys, Identifier.class), toSet()));
		
		waiter.putCache(keys, result);
		LOGGER.debug("Indexing finished for {} on {}.", alias, keys);
		waiter.done();
		return result;
	}
}
