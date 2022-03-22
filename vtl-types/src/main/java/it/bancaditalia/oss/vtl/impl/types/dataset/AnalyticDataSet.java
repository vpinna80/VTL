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

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.Utils;

public final class AnalyticDataSet<TT> extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticDataSet.class);
	private final DataSet source;
	private final Set<DataStructureComponent<Identifier, ?, ?>> partitionIds;
	private final Comparator<DataPoint> orderBy;
	private final Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors;
	private final Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers;
	private final Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components;
	private final int inf;
	private final int sup;
	private final SerFunction<DataPoint, Lineage> lineageOp;

	public AnalyticDataSet(DataSet source, DataSetMetadata newStructure, SerFunction<DataPoint, Lineage> lineageOp, 
			Set<DataStructureComponent<Identifier, ?, ?>> partitionIds, Comparator<DataPoint> orderBy, WindowCriterion range,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors, 
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers, 
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components)
	{
		super(newStructure);
		
		this.source = source;
		this.lineageOp = lineageOp;
		this.partitionIds = partitionIds;
		this.orderBy = orderBy;
		this.collectors = collectors;
		this.finishers = finishers;
		this.components = components;
		
		if (range != null)
		{
			LimitCriterion infBound = range.getInfBound(), supBound = range.getSupBound();
			inf = (infBound.getDirection() == PRECEDING ? -1 : 1) * (int) infBound.getCount(); 
			sup = (supBound.getDirection() == PRECEDING ? -1 : 1) * (int) supBound.getCount();
		}
		else
		{
			inf = Integer.MIN_VALUE;
			sup = Integer.MAX_VALUE;
		}
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		Stream<Stream<DataPoint>> original;
		
		// when partition by all, each window has a single data point in it (though the result can have any number) 
		if (partitionIds.equals(getComponents(Identifier.class)))
			original = source.stream()
					.map(Collections::singletonList)
					.map(this::applyToWindow);
		else
			original = Utils.getStream(source.stream()
					.collect(groupingByConcurrent(dp -> dp.getValues(partitionIds), 
							collectingAndThen(toList(), this::applyToWindow)))
					.values());
		
		Stream<DataPoint> result = original.collect(concatenating(ORDERED));
		
		if (LOGGER.isDebugEnabled())
			result = result.peek(dp -> {
				LOGGER.trace("Analytic invocation output datapoint {}", dp);
			});
		
		return result;
	}

	private Stream<DataPoint> applyToWindow(List<DataPoint> windowSet)
	{
		// Sorted window
		DataPoint[] window = windowSet.toArray(new DataPoint[windowSet.size()]);
		Arrays.sort(window, orderBy);
		
		LOGGER.debug("Analyzing window of {} datapoints with keys {}", window.length, window[0].getValues(partitionIds));
		if (LOGGER.isTraceEnabled())
			Arrays.stream(window).forEach(dp -> LOGGER.trace("\tcontaining {}", dp));
		
		IntStream indexes = IntStream.range(0, window.length);
		if (!Utils.SEQUENTIAL)
			indexes = indexes.parallel();
		
		return indexes.mapToObj(index -> applyAtIndex(window, index))
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

	private Entry<Map<DataStructureComponent<?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>, DataPoint> applyAtIndex(DataPoint[] window, int index)
	{
		int safeInf = max(0, safeSum(index, inf));
		int safeSup = 1 + min(window.length - 1, safeSum(index, sup));
		
		Map<DataStructureComponent<?, ?, ?>, Stream<ScalarValue<?, ?, ?, ?>>> ranges = new HashMap<>();
		for (DataStructureComponent<?, ?, ?> component: components.keySet())
		{
			Stream<ScalarValue<?, ?, ?, ?>> stream = safeInf < safeSup ? Arrays.stream(window, safeInf, safeSup).map(dp -> dp.get(component)) : Stream.empty();
			if (!Utils.SEQUENTIAL)
				stream = stream.parallel();
			ranges.put(component, stream);
		}
		
		LOGGER.trace("\tAnalysis over {} datapoints for datapoint {}", safeSup - safeInf, window[index]);
		
		final Map<DataStructureComponent<?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>> atIndex = Utils.getStream(components)
			.map(splitting((oldC, newC) -> {  
				// get the array slice containing all the datapoints in current window
				Stream<ScalarValue<?, ?, ?, ?>> stream = ranges.get(oldC);
				
				// Collector to compute the invocation over current range for the specified component
				SerCollector<ScalarValue<?, ?, ?, ?>, ?, Collection<ScalarValue<?, ?, ?, ?>>> collector = collectingAndThen(collectors.get(oldC), 
					v -> finishers.get(oldC).apply(v, window[index].get(oldC)));
				
				if (LOGGER.isTraceEnabled())
					collector = teeing(toList(), collector, (source, result) -> {
						LOGGER.trace("Result on component {} with values {} yield {}", newC, source, result);
						return result;
					});

				// Pair the result with the new measure
				return new SimpleEntry<>(newC, stream.collect(collector));
			})).collect(entriesToMap());
		
		return new SimpleEntry<>(atIndex, window[index]);
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