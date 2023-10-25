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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentSet;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.lang.ref.SoftReference;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.Utils;

public final class AnalyticDataSet<TT> extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticDataSet.class);
	
	private static final Map<Entry<Set<DataStructureComponent<Identifier, ?, ?>>, List<SortCriterion>>, WeakHashMap<DataSet, SoftReference<Collection<DataPoint[]>>>> CACHES = new ConcurrentHashMap<>();
	
	private final DataSet source;
	private final Set<DataStructureComponent<Identifier, ?, ?>> partitionIds;
	private final Comparator<DataPoint> orderBy;
	private final Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors;
	private final Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers;
	private final Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components;
	private final int inf;
	private final int sup;
	private final SerFunction<DataPoint, Lineage> lineageOp;
	
	private final transient WeakHashMap<DataSet, SoftReference<Collection<DataPoint[]>>> cache;

	public AnalyticDataSet(DataSet source, DataSetMetadata newStructure, SerFunction<DataPoint, Lineage> lineageOp, WindowClause clause,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors, 
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers, 
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components)
	{
		super(newStructure);
		
		this.source = source;
		this.lineageOp = lineageOp;
		this.partitionIds = clause.getPartitioningIds();
		this.collectors = collectors;
		this.finishers = finishers;
		this.components = components;
		
		this.orderBy = (dp1, dp2) -> {
				for (SortCriterion criterion: clause.getSortCriteria())
				{
					int res = dp1.get(criterion.getComponent()).compareTo(dp2.get(criterion.getComponent()));
					if (res != 0)
						return criterion.getMethod() == ASC ? res : -res;
				}

				return 0;
			};

		WindowCriterion window = clause.getWindowCriterion();
		if (window != null)
		{
			LimitCriterion infBound = window.getInfBound(), supBound = window.getSupBound();
			inf = (infBound.getDirection() == PRECEDING ? -1 : 1) * (int) infBound.getCount(); 
			sup = (supBound.getDirection() == PRECEDING ? -1 : 1) * (int) supBound.getCount();
		}
		else
		{
			inf = Integer.MIN_VALUE;
			sup = Integer.MAX_VALUE;
		}

		this.cache = CACHES.computeIfAbsent(new SimpleEntry<>(partitionIds, clause.getSortCriteria()), c -> {
			LOGGER.info("Creating cache for partitioning {}@{} with clause #{}", source.getClass().getSimpleName(), source.hashCode(), clause.hashCode());
			return new WeakHashMap<>();
		});
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		Stream<Stream<DataPoint>> original;
		
		// when partition by all, each window has a single data point in it (though the result can have any number) 
		if (partitionIds.equals(getMetadata().getIDs()))
			original = source.stream()
					.map(v -> new DataPoint[] { v })
					.map(this::applyToPartition);
		else
		{
			Collection<DataPoint[]> partitioned;
			
			synchronized (cache)
			{
				SoftReference<Collection<DataPoint[]>> cacheEntry = cache.get(source);
				partitioned = cacheEntry != null ? cacheEntry.get() : null;
			}
			
			if (partitioned == null)
			{
				LOGGER.debug("Caching partitioning for {}@{}", source.getClass().getSimpleName(), source.hashCode());
				partitioned = source.stream()
					.collect(groupingByConcurrent(dp -> dp.getValues(partitionIds), collectingAndThen(toList(), list -> {
							DataPoint[] window = list.toArray(new DataPoint[list.size()]);
							Arrays.sort(window, orderBy); 
							return window;
						}))
					).values();

				synchronized (cache)
				{
					cache.put(source, new SoftReference<>(partitioned));
				}
			}
			else
			{
				LOGGER.debug("Using cached partitioning for {}@{}", source.getClass().getSimpleName(), source.hashCode());
			}
			
			original = Utils.getStream(partitioned)
					.map(this::applyToPartition);
		}
		
		Stream<DataPoint> result = original.collect(concatenating(ORDERED));
		
		if (LOGGER.isDebugEnabled())
			result = result.peek(dp -> {
				LOGGER.trace("Analytic invocation output datapoint {}", dp);
			});
		
		return result;
	}

	private Stream<DataPoint> applyToPartition(DataPoint[] partition)
	{
		LOGGER.debug("Analyzing partition of {} datapoints with keys {}", partition.length, partition[0].getValues(partitionIds));
		if (LOGGER.isTraceEnabled())
			Arrays.stream(partition).forEach(dp -> LOGGER.trace("\tcontaining {}", dp));
		
		IntStream indexes = IntStream.range(0, partition.length);
		if (!Utils.SEQUENTIAL)
			indexes = indexes.parallel();
		
		return indexes.mapToObj(index -> applyToWindow(partition, index))
			.map(splitting(AnalyticDataSet::explode))
			.collect(concatenating(ORDERED))
			.map(splitting((values, dp) -> new DataPointBuilder(dp)
				.delete(components.keySet())
				.addAll(values)
				.build(lineageOp.apply(dp), getMetadata())));
	}

	// Explode the collections resulting from the application of the window function to single components
	private static Stream<Entry<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint>> explode(
			Map<DataStructureComponent<?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>> colls, DataPoint original)
	{
		// Shortcut when analytic mapping 	is 1:1
		if (Utils.getStream(colls.values()).allMatch(coll -> coll.size() == 1))
		{
			return Stream.of(new SimpleEntry<>(Utils.getStream(colls)
					.map(Utils.keepingKey(coll -> coll.iterator().next()))
					.collect(entriesToMap()), original));
		}
		
		final Stream<Entry<DataStructureComponent<?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> stream = Utils.getStream(colls);
		Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> collected = stream.collect(
				SerCollector.of(() -> (Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>>) new HashSet<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>>(),
					(acc, cEntry) -> {
						DataStructureComponent<?, ?, ?> comp = cEntry.getKey();
						if (!acc.isEmpty())
						{
							Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> accBefore = new HashSet<>(acc);
							acc.clear();
							for (ScalarValue<?, ?, ?, ?> cVal: cEntry.getValue())
								accBefore.forEach(map -> {
									map = new HashMap<>(map);
									map.put(cEntry.getKey(), cVal);
									acc.add(map);
								});
						}
						else
							acc.addAll(cEntry.getValue().stream()
									.<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>>map(cVal -> singletonMap(comp, cVal))
									.collect(toConcurrentSet()));
					}, (accLeft, accRight) -> {
						if (accLeft.isEmpty() && accRight.isEmpty())
							return accLeft;
						else if (accLeft.isEmpty())
							return accRight;
						else if (accRight.isEmpty())
							return accLeft;
						
						// Merge each of the maps on the left with each of the maps on the right
						return Utils.getStream(accLeft)
							.map(mapLeft -> Utils.getStream(accRight)
								.map(mapRight -> {
									HashMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>(mapLeft);
									result.putAll(mapRight);
									return result;
								})
							).collect(concatenating(ORDERED))
							.collect(toConcurrentSet());
					}, EnumSet.of(UNORDERED)));
		
		if (LOGGER.isTraceEnabled())
			LOGGER.trace("Analytic produced {} for datapoint {}:", collected, original);
		
		return Utils.getStream(collected)
			.map(toEntryWithValue(map -> original));
	}

	private Entry<Map<DataStructureComponent<?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>, DataPoint> applyToWindow(DataPoint[] partition, int index)
	{
		int safeInf = max(0, safeSum(index, inf));
		int safeSup = 1 + min(partition.length - 1, safeSum(index, sup));
		
		Map<DataStructureComponent<?, ?, ?>, Stream<ScalarValue<?, ?, ?, ?>>> windows = new HashMap<>();
		for (DataStructureComponent<?, ?, ?> component: components.keySet())
		{
			Stream<ScalarValue<?, ?, ?, ?>> stream = safeInf < safeSup ? Arrays.stream(partition, safeInf, safeSup).map(dp -> dp.get(component)) : Stream.empty();
			if (!Utils.SEQUENTIAL)
				stream = stream.parallel();
			windows.put(component, stream);
		}
		
		LOGGER.trace("\tAnalysis over {} datapoints for datapoint {}", safeSup - safeInf, partition[index]);
		
		final Map<DataStructureComponent<?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>> atIndex = Utils.getStream(components)
			.map(splitting((oldC, newC) -> {  
				// get the array slice containing all the datapoints in current window
				Stream<ScalarValue<?, ?, ?, ?>> window = windows.get(oldC);
				
				// Collector to compute the invocation over current range for the specified component
				SerCollector<ScalarValue<?, ?, ?, ?>, ?, Collection<ScalarValue<?, ?, ?, ?>>> collector = collectingAndThen(collectors.get(oldC), 
					v -> finishers.get(oldC).apply(v, partition[index].get(oldC)));
				
				if (LOGGER.isTraceEnabled())
					collector = teeing(toList(), collector, (source, result) -> {
						LOGGER.trace("Result on component {} with values {} yield {}", newC, source, result);
						return result;
					});

				// Pair the result with the new measure
				return new SimpleEntry<>(newC, window.collect(collector));
			})).collect(entriesToMap());
		
		return new SimpleEntry<>(atIndex, partition[index]);
	}

	protected static int safeInc(int a)
	{
		return safeSum(a, 1);
	}

	/*
	 * Detects overf)lows in sum and caps it to Integer.MAX_VALUE 
	 */
	protected static int safeSum(int x, int y)
	{
		int r = x + y;
		return ((x ^ r) & (y ^ r)) < 0 ? Integer.MAX_VALUE : r;
	}
}