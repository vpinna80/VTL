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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.DESC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.DATAPOINTS;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentSet;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.lang.ref.SoftReference;
import java.security.InvalidParameterException;
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

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
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
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticDataSet.class);
	
	private static final Map<Entry<Set<? extends DataSetComponent<?, ?, ?>>, List<SortCriterion>>, WeakHashMap<DataSet, SoftReference<Collection<DataPoint[]>>>> CACHES = new ConcurrentHashMap<>();
	
	private final DataSet source;
	private final Set<? extends DataSetComponent<?, ?, ?>> partitionIds;
	private final Comparator<DataPoint> orderBy;
	private final List<SortCriterion> sortCriteria;
	private final WindowCriterion window;
	private final SerUnaryOperator<Lineage> lineageOp;
	private final DataSetComponent<?, ?, ?> srcComponent;
	private final DataSetComponent<?, ?, ?> destComponent;
	private final SerFunction<DataPoint, T> extractor;
	private final SerCollector<T, ?, TT> collector;
	private final SerBiFunction<TT, T, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisher;

	private final transient WeakHashMap<DataSet, SoftReference<Collection<DataPoint[]>>> cache;


	public AnalyticDataSet(DataSet source, DataSetStructure structure, SerUnaryOperator<Lineage> lineageOp, WindowClause clause,
			DataSetComponent<?, ?, ?> srcComponent, DataSetComponent<?, ?, ?> destComponent,
			SerFunction<DataPoint, T> extractor,
			SerCollector<T, ?, TT> collector, 
			SerBiFunction<TT, T, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisher)
	{
		super(structure);
		
		this.source = source;
		this.lineageOp = lineageOp;
		this.srcComponent = srcComponent;
		this.destComponent = destComponent;
		this.extractor = coalesce(extractor, dp -> (T) dp.get(srcComponent));
		this.collector = collector;
		this.partitionIds = clause.getPartitioningComponents();
		this.sortCriteria = clause.getSortCriteria();
		this.finisher = finisher;
		this.window = clause.getWindowCriterion();
		
		this.orderBy = (dp1, dp2) -> {
				for (SortCriterion criterion: clause.getSortCriteria())
				{
					DataSetComponent<?, ?, ?> comp = criterion.getComponent();
					int res = dp1.get(comp).compareTo(dp2.get(comp));
					if (res != 0)
						return criterion.getMethod() == ASC ? res : -res;
				}

				return 0;
			};


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
							if (sortCriteria.size() > 0)
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
		
		IntStream indexes = Utils.getStream(partition.length);
		
		// performance shortcut for single-valued analytic functions 
		Stream<Entry<TT, DataPoint>> exploded = indexes.mapToObj(index -> applyToWindow(partition, index));
		Stream<? extends Entry<?, DataPoint>> finished = exploded;
		if (finisher != null)
			finished = exploded.map(splitting(this::explode))
				.collect(concatenating(ORDERED));

		DataSetStructure destStructure = getMetadata();
		boolean remove = srcComponent.getAlias().equals(destComponent.getAlias());
		return finished.map(e -> new SimpleEntry<>((ScalarValue<?, ?, ?, ?>) e.getKey(), e.getValue()))
			.map(splitting((value, dp) -> {
				DataPointBuilder builder = new DataPointBuilder(dp, DONT_SYNC);
				if (remove)
					builder = builder.delete(srcComponent);
					
				builder = builder.delete(destComponent).add(destComponent, value);
				return builder.build(lineageOp.apply(dp.getLineage()), destStructure);
			}));
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
		LimitCriterion infBound = window.getInfBound();
		LimitCriterion supBound = window.getSupBound();

		int inf, sup;
		
		if (window.getType() == DATAPOINTS)
		{
			if (infBound.isUnbounded())
				inf = 0;
			else
			{
				int safeInf = (infBound.getDirection() == PRECEDING ? -1 : 1) * (int) infBound.getCount(); 
				inf = infBound.isUnbounded() ? 0 : max(0, index + safeInf);
			}

			if (supBound.isUnbounded())
				sup = partition.length;
			else
			{
				int safeSup = (supBound.getDirection() == PRECEDING ? -1 : 1) * (int) supBound.getCount();
				sup = supBound.isUnbounded() ? partition.length : min(partition.length, 1 + index + safeSup);
			}
		}
		else
		{
			if (sortCriteria.size() == 0)
			{
				inf = 0;
				sup = partition.length;
			}
			else if ((infBound.isUnbounded() || infBound.getCount() == 0) && 
				(supBound.isUnbounded() || supBound.getCount() == 0))
			{
				Set<DataSetComponent<?, ?, ?>> comps = sortCriteria.stream().map(SortCriterion::getComponent).collect(toSet());
				
				if (infBound.isUnbounded())
					inf = 0;
				else if (infBound.getCount() == 0)
				{
					Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> refValues = partition[index].getValues(comps);
					
					inf = index;
					while (inf >= 0)
					{
						if (!refValues.equals(partition[inf].getValues(comps)))
							break;
						
						inf--;
					}
				}
				else
					throw new InvalidParameterException("The frame specification is invalid: " + window);
				
				if (supBound.isUnbounded())
					sup = partition.length;
				else if (supBound.getCount() == 0)
				{
					Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> refValues = partition[index].getValues(comps);
					
					sup = index;
					while (sup < partition.length)
					{
						if (!refValues.equals(partition[sup].getValues(comps)))
							break;
						
						sup++;
					}
				}
				else
					throw new InvalidParameterException("The frame specification is invalid: " + window);
			}
			else if (sortCriteria.size() == 1)
			{
				SortCriterion sortCriterion = sortCriteria.get(0);
				DataSetComponent<?, ?, ?> sortKey = sortCriterion.getComponent();
				if (!(sortKey instanceof NumberDomainSubset))
					throw new VTLIncompatibleTypesException(window.toString(), sortKey, NUMBERDS);
				
				double currentKeyValue = ((Number) partition[index].get(sortKey).get()).doubleValue();

				if (infBound.isUnbounded())
					inf = 0;
				else
				{
					boolean direction = infBound.getDirection() == PRECEDING ^ sortCriterion.getMethod() == DESC;
					int increment = direction ? -1 : 1;
					double rangeInf = currentKeyValue + increment * (int) infBound.getCount();
					
					inf = index;
					while (inf >= 0 && inf < partition.length)
					{
						if (direction && ((Number) partition[inf].get(sortKey).get()).doubleValue() < rangeInf)
							break;
						else if (!direction && ((Number) partition[inf].get(sortKey).get()).doubleValue() > rangeInf)
							break;

						inf += increment;
					}
				}

				if (supBound.isUnbounded())
					sup = partition.length;
				else
				{
					boolean direction = supBound.getDirection() == PRECEDING ^ sortCriterion.getMethod() == DESC;
					int increment = direction ? -1 : 1;
					double rangeSup = currentKeyValue + increment * (int) supBound.getCount();
					
					sup = index;
					while (sup >= 0 && sup < partition.length)
					{
						if (direction && ((Number) partition[sup].get(sortKey).get()).doubleValue() < rangeSup)
							break;
						else if (!direction && ((Number) partition[sup].get(sortKey).get()).doubleValue() > rangeSup)
							break;

						sup += increment;
					}
				}
				
				if (sup < inf)
				{
					int tmp = sup;
					sup = inf;
					inf = tmp;
				}
			}
			else
				throw new InvalidParameterException("The frame specification is invalid: " + window);
		}

		LOGGER.trace("\tAnalysis over {} datapoints for datapoint {}", sup - inf, partition[index]);
		Stream<DataPoint> frame = inf < sup ? Arrays.stream(partition, inf, sup) : Stream.empty();

		return new SimpleEntry<>(processSingleComponent(partition[index], frame), partition[index]);
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
}