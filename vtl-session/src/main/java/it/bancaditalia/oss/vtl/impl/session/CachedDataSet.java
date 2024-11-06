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

import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
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
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.Utils;

public class CachedDataSet extends NamedDataSet
{
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(CachedDataSet.class);
	private static final WeakHashMap<VTLSession, Map<VTLAlias, CacheWaiter>> SESSION_CACHES = new WeakHashMap<>(); 
	private static final ReferenceQueue<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>> REF_QUEUE = new ReferenceQueue<>();
	private static final Map<Reference<?>, Entry<VTLAlias, Set<DataStructureComponent<Identifier, ?, ?>>>> REF_NAMES = new ConcurrentHashMap<>();

	private final transient CacheWaiter waiter;
	private transient volatile SoftReference<Set<DataPoint>> unindexed = new SoftReference<>(null);
	
	static {
		new Thread() {
			
			{
				setName("CachedDataSet gc watcher");
				setDaemon(true);
			}
			
			@Override
			public void run() 
			{
				while (!Thread.interrupted())
					try 
					{
						Reference<? extends Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>> ref;
						while ((ref = REF_QUEUE.poll()) != null)
						{
							Entry<VTLAlias, Set<DataStructureComponent<Identifier, ?, ?>>> data = REF_NAMES.remove(ref);
							if (data != null)
								LOGGER.warn("Cleaned an index of {} over {}", data.getKey(), data.getValue());
						}
						
						Thread.sleep(5000);
					}
					catch (InterruptedException e)
					{
						Thread.currentThread().interrupt();
					}
			}
			
		}.start();
	}

	/**
	 * Waits for cache completion on a dataset if caching has already started.
	 * Prevents different threads from starting to cache the same dataset.
	 *  
	 * @author m027907
	 */
	private static class CacheWaiter implements ManagedBlocker
	{
		private final Semaphore semaphore = new Semaphore(1);
		private final Map<Set<DataStructureComponent<Identifier, ?, ?>>, SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>>> cache = new ConcurrentHashMap<>();
		
		private final transient VTLAlias alias;
		private transient AtomicReference<Thread> lockingRef = new AtomicReference<>();

		public CacheWaiter(VTLAlias alias, Set<DataStructureComponent<Identifier, ?, ?>> ids)
		{
			List<Set<DataStructureComponent<Identifier, ?, ?>>> accumulator = new ArrayList<>();
			this.alias = alias;
			accumulator.add(emptySet());
			// build all ids subsets
			for (DataStructureComponent<Identifier, ?, ?> key: ids)
				accumulator = accumulator.stream()
					.flatMap(set -> {
						Set<DataStructureComponent<Identifier, ?, ?>> newSet = new HashSet<>(set);
						newSet.add(key);
						return Stream.of(set, newSet);
					}).collect(toCollection(ArrayList::new));
			for (Set<DataStructureComponent<Identifier, ?, ?>> keySet: accumulator)
				cache.put(keySet, new SoftReference<>(null));
		}
		
		@Override
		public boolean block() throws InterruptedException
		{
			LOGGER.trace("++++ Acquiring semaphore for {}", alias);
			Thread currentThread = Thread.currentThread();
			int count = 0;
			Thread lockingThread;
			while ((lockingThread = lockingRef.get()) != currentThread)
				if (count++ > 0 && semaphore.tryAcquire(500, MILLISECONDS))
					return lockingRef.compareAndSet(lockingThread, currentThread);
				else if (count > 10)
					return false;
			return true;
		}

		@Override
		public boolean isReleasable()
		{
			if (Thread.currentThread() == lockingRef.get())
				return true;
			final boolean tryAcquire = semaphore.tryAcquire();
			if (tryAcquire)
			{
				LOGGER.trace("++++ Acquired semaphore for {}", alias);
				lockingRef.set(Thread.currentThread());
			}
			return tryAcquire;
		}
		
		/**
		 * Store a cache for grouped datapoints of this dataset over specified keys 
		 * @param keys keys to group over
		 * @param newCache the grouped datapoints
		 */
		public void putCache(Set<DataStructureComponent<Identifier, ?, ?>> keys, Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> newCache)
		{
			SoftReference<Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>>> cacheRef = new SoftReference<>(newCache, REF_QUEUE);
			REF_NAMES.put(cacheRef, new SimpleEntry<>(alias, keys));
			cache.put(keys, cacheRef);
		}
		
		public Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> getCache(Set<DataStructureComponent<Identifier, ?, ?>> keys)
		{
			return cache.get(keys).get();
		}
		
		public void done()
		{
			LOGGER.trace("---- Releasing semaphore for {}", alias);
			semaphore.release();
			lockingRef.set(null);
		}
	}

	public CachedDataSet(VTLSessionImpl session, VTLAlias alias, DataSet delegate)
	{
		super(alias, delegate);
		
		synchronized (SESSION_CACHES)
		{
			Map<VTLAlias, CacheWaiter> waitersMap = SESSION_CACHES.get(session);
			if (waitersMap == null)
			{
				waitersMap = new ConcurrentHashMap<>();
				SESSION_CACHES.put(session, waitersMap);
			}
			waiter = waitersMap.computeIfAbsent(alias, a -> new CacheWaiter(alias, getMetadata().getIDs()));
		}
	}

	public CachedDataSet(VTLSessionImpl session, NamedDataSet delegate)
	{
		this(session, delegate.getAlias(), delegate.getDelegate());
	}

	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter, SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<? super TT, ? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> value = waiter.getCache(keys);
		if (value == null)
		{
			if (!lock())
				return Stream.empty();
			
			value = waiter.getCache(keys);
			if (value == null)
				value = createCache(keys);
			else
			{
				LOGGER.debug("Cache waited hit for {}.", getAlias());
				waiter.done();
			}
		}
		else
		{
			LOGGER.debug("Cache hit for {}.", getAlias());
			waiter.done();
		}
			
		Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filterOutsideKeys = new HashMap<>(filter);
		filterOutsideKeys.keySet().retainAll(keys);
		boolean noFilter = filterOutsideKeys.isEmpty();
		
		return Utils.getStream(value)
			.filter(entryByKey(idVals -> idVals.entrySet().containsAll(filter.entrySet())))
			.map(keepingKey(Set::stream))
			.map(keepingKey(s -> s.filter(dp -> noFilter || dp.matches(filter))))
			.map(keepingKey(s -> s.collect(groupCollector)))
			.map(splitting((k, v) -> finisher.apply(v, k)));
	}
	
	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		if (!lock())
			return Stream.empty();

		Set<DataPoint> cache = unindexed.get();
		if (cache != null)
		{
			waiter.done();
			return cache.stream();
		}
		else
			return getUnindexedCache(true);
	}

	@Override
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, SerBiPredicate<DataPoint, DataPoint> predicate, SerBinaryOperator<DataPoint> mergeOp, boolean leftJoin)
	{
		if (!lock())
			return new StreamWrapperDataSet(metadata, Stream::empty);
	
		Set<DataStructureComponent<Identifier, ?, ?>> commonIds = new HashSet<>(getMetadata().getIDs());
		commonIds.retainAll(other.getMetadata().getIDs());
		
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
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> finalValue = value;
		
		return new AbstractDataSet(metadata)
		{
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return other.stream()
						.map(dpThis -> flatMapDataPoint(newPredicate, newMergeOp, commonIds, finalValue, leftJoin, dpThis))
						.collect(concatenating(ORDERED));
			}
		};
	}

	private boolean lock()
	{
		try
		{
			ForkJoinPool.managedBlock(waiter);
			return true;
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			return false;
		}
	}

	protected Stream<DataPoint> getUnindexedCache(boolean unlockWhenComplete)
	{
		VTLAlias alias = getAlias();

		LOGGER.debug("Cache miss for {}, start caching.", alias);
		Set<DataPoint> cache = unindexed.get();
		if (cache != null)
			return cache.stream();
		
		unindexed = new SoftReference<>(newSetFromMap(new ConcurrentHashMap<>()));
		
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
					LOGGER.warn("Caching interrupted for {}.", alias);
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

	protected Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> createCache(Set<DataStructureComponent<Identifier, ?, ?>> keys)
	{
		VTLAlias alias = getAlias();
		LOGGER.debug("Index miss for {}, start indexing on {}.", alias, keys);

		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> result;
		try (Stream<DataPoint> stream = getUnindexedCache(false))
		{
			if (getMetadata().getIDs().equals(keys))
				result = stream.collect(toConcurrentMap(dp -> dp.getValues(keys, Identifier.class), Collections::singleton));
			else
				result = stream.collect(groupingByConcurrent(dp -> dp.getValues(keys, Identifier.class), toSet()));
		}
		
		waiter.putCache(keys, result);
		LOGGER.debug("Indexing finished for {} on {}.", alias, keys);
		waiter.done();
		return result;
	}
	
	@Override
	public boolean isCacheable()
	{
		return false;
	}
}
