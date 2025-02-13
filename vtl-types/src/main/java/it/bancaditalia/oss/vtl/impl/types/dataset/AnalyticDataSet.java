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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentSet;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.lang.ref.SoftReference;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
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
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public final class AnalyticDataSet<T, TT> extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticDataSet.class);
	
	private static final Map<Entry<Set<? extends DataStructureComponent<?, ?, ?>>, List<SortCriterion>>, WeakHashMap<DataSet, SoftReference<Collection<DataPoint[]>>>> CACHES = new ConcurrentHashMap<>();
	
	private final DataSet source;
	private final Set<? extends DataStructureComponent<?, ?, ?>> partitionIds;
	private final Comparator<DataPoint> orderBy;
	private final int inf;
	private final int sup;
	private final SerUnaryOperator<Lineage> lineageOp;
	private final DataStructureComponent<?, ?, ?> destComponent;
	private final SerFunction<DataPoint, T> extractor;
	private final SerCollector<T, ?, TT> collector;
	private final SerBiFunction<TT, T, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisher;

	private final transient WeakHashMap<DataSet, SoftReference<Collection<DataPoint[]>>> cache;

	public AnalyticDataSet(DataSet source, DataSetMetadata structure, SerUnaryOperator<Lineage> lineageOp, WindowClause clause,
			DataStructureComponent<?, ?, ?> srcComponent, DataStructureComponent<?, ?, ?> destComponent,
			SerFunction<DataPoint, T> extractor,
			SerCollector<T, ?, TT> collector, 
			SerBiFunction<TT, T, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisher)
	{
		super(structure);
		
		this.source = source;
		this.lineageOp = lineageOp;
		this.destComponent = destComponent;
		this.extractor = coalesce(extractor, dp -> (T) dp.get(srcComponent));
		this.collector = collector;
		this.partitionIds = clause.getPartitioningComponents();
		this.finisher = finisher;
		
		this.orderBy = (dp1, dp2) -> {
				for (SortCriterion criterion: clause.getSortCriteria())
				{
					DataStructureComponent<?, ?, ?> comp = criterion.getComponent();
					int res = dp1.get(comp).compareTo(dp2.get(comp));
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
			LOGGER.info("Creating cache for partitioning {}{} with clause #{}", source.getClass().getSimpleName(), source.getMetadata(), clause.hashCode());
			return new WeakHashMap<>();
		});
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		Stream<Stream<DataPoint>> original;
		
		// when partition by all, each window has a single data point in it (though the result can have any number) 
		if (partitionIds.equals(source.getMetadata().getIDs()))
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
			
			original = Utils.getStream(partitioned).map(this::applyToPartition);
		}
		
		Stream<DataPoint> result = original.collect(concatenating(ORDERED));
		
		if (LOGGER.isDebugEnabled())
			result = result.peek(dp -> LOGGER.trace("Analytic invocation output datapoint {}", dp));
		
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
		
		// performance shortcut for single-valued analytic functions 
		Stream<Entry<TT, DataPoint>> exploded = indexes.mapToObj(index -> applyToWindow(partition, index));
		Stream<? extends Entry<?, DataPoint>> finished = exploded;
		if (finisher != null)
			finished = exploded.map(splitting(this::explode))
				.collect(concatenating(ORDERED));
		
		return finished.map(e -> new SimpleEntry<>((ScalarValue<?, ?, ?, ?>) e.getKey(), e.getValue()))
			.map(splitting((value, dp) -> new DataPointBuilder(dp, DONT_SYNC)
				.delete(destComponent)
				.add(destComponent, value)
				.build(lineageOp.apply(dp.getLineage()), getMetadata())));
	}

	// Explode the collections resulting from the application of the window function to single components
	private Stream<Entry<ScalarValue<?, ?, ?, ?>, DataPoint>> explode(TT collected, DataPoint original)
	{
		return Utils.getStream((Collection<?>) collected).map(ScalarValue.class::cast).map(k -> {
			Entry<ScalarValue<?, ?, ?, ?>, DataPoint> entry = new SimpleEntry<>(k, original);
			return entry;
		});
	}

	// computes the result(s) for a component in a given row of the window
	private Entry<TT, DataPoint> applyToWindow(DataPoint[] partition, int index)
	{
		int safeInf = max(0, safeSum(index, inf));
		int safeSup = 1 + min(partition.length - 1, safeSum(index, sup));
		
		Stream<DataPoint> window = safeInf < safeSup ? Arrays.stream(partition, safeInf, safeSup) : Stream.empty();
		
		LOGGER.trace("\tAnalysis over {} datapoints for datapoint {}", safeSup - safeInf, partition[index]);
		var processed = processSingleComponent(partition[index], window);
		
		return new SimpleEntry<>(processed, partition[index]);
	}
	
	private TT processSingleComponent(DataPoint dp, Stream<DataPoint> window)
	{
		// Collector to compute the invocation over current range for the specified component
		T extracted = extractor.apply(dp);
		SerCollector<T, ?, TT> withFinisher;
		if (finisher == null)
			withFinisher = collector;
		else
			withFinisher = collectingAndThen(collector, v -> (TT) finisher.apply(v, extracted));
		
		if (LOGGER.isTraceEnabled())
			withFinisher = teeing(toConcurrentSet(), withFinisher, (source, result) -> {
				LOGGER.trace("Result on component {} with values {} yield {}", destComponent, source, result);
				return result;
			});
		
		return window.map(extractor).collect(withFinisher);
	}

	protected static int safeInc(int a)
	{
		return safeSum(a, 1);
	}

	/*
	 * Detects overflows in sum and caps it to Integer.MAX_VALUE 
	 */
	protected static int safeSum(int x, int y)
	{
		int r = x + y;
		return ((x ^ r) & (y ^ r)) < 0 ? Integer.MAX_VALUE : r;
	}
}