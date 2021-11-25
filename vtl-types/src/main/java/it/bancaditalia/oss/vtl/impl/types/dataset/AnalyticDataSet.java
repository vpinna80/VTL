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

import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithKey;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.Utils;

public final class AnalyticDataSet<TT> extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticDataSet.class);

	private final DataSet source; 
	private final Set<DataStructureComponent<Identifier, ?, ?>> ids;
	private final Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers;
	private final int sup;
	private final Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components;
	private final int inf;
	private final Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors;
	private final SerCollector<DataPoint, ConcurrentSkipListSet<DataPoint>, ConcurrentSkipListSet<DataPoint>> toSortedSet;

	public AnalyticDataSet(DataSet source, DataSetMetadata newStructure, Set<DataStructureComponent<Identifier, ?, ?>> ids,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors,
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers,
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components,
			int inf,
			int sup,
			SerCollector<DataPoint, ConcurrentSkipListSet<DataPoint>, ConcurrentSkipListSet<DataPoint>> sortingCollector)
	{
		super(newStructure);
		
		this.source = source;
		this.ids = ids;
		this.finishers = finishers;
		this.sup = sup;
		this.components = components;
		this.inf = inf;
		this.collectors = collectors;
		this.toSortedSet = sortingCollector;
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return source.streamByKeys(ids, toSortedSet, (part, keyValues) -> applyToWindow(part))
				.collect(concatenating(ORDERED));
	}

	private Stream<DataPoint> applyToWindow(ConcurrentSkipListSet<DataPoint> part)
	{
		// Sorted window
		DataPoint[] partArray = part.toArray(new DataPoint[part.size()]);
		
		IntStream indexes = IntStream.range(0, partArray.length);
		if (!Utils.SEQUENTIAL)
			indexes = indexes.parallel();
		
		return indexes.mapToObj(index -> applyAtIndex(partArray, index))
			.map(splitting(AnalyticDataSet::explode))
			.collect(concatenating(ORDERED))
			.map(splitting((values, dp) -> new DataPointBuilder(dp)
				.delete(components.keySet())
				.addAll(values)
				.build(dp.getLineage(), getMetadata())));
	}

	// Explode the collections resulting from the application of the window function to single components
	private static Stream<Entry<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint>> explode(
			Map<? extends DataStructureComponent<?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>> colls, DataPoint original)
	{
		final Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> collect = Utils.getStream(colls)
			.map(coll -> Utils.getStream(coll.getValue()).map(toEntryWithKey(v -> coll.getKey())).collect(toList()))
			// Generate all possible combinations of measure results of the window function
			.collect(SerCollector.of(HashSet::new, 
					// Accumulator: combine all the values for a measure with each of the existing results
					(a, l) -> {
						Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> results = new HashSet<>();
						for (Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> acc: a)
							for (Entry<? extends DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> e: l)
							{
								Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> combination = new HashMap<>(acc);
								combination.put(e.getKey(), e.getValue());
								results.add(combination);
							};
						a.clear();
						a.addAll(results);						
					}, 
					// Combiner: generate a new map combining all measure values from the first map to all measure values of the second
					(a1, a2) -> {
						HashSet<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> results = new HashSet<>();
						for (Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> acc1: a1)
							for (Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> acc2: a2)
							{
								Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> combination = new HashMap<>(acc1);
								combination.putAll(acc2);
								results.add(combination);
							};
						return results;
					}, 
					EnumSet.of(UNORDERED))
			);
		
		if (LOGGER.isTraceEnabled())
		{
			LOGGER.trace("Analytic invocation for datapoint {} returned:", original);
			collect.stream().forEach(map -> LOGGER.trace("\t --> {}", map));
		}
		
		return Utils.getStream(collect)
			.map(toEntryWithValue(map -> original));
	}

	private Entry<Map<? extends DataStructureComponent<?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>, DataPoint> applyAtIndex(DataPoint[] partArray, int index)
	{
		final int safeInf = max(0, safeSum(index, inf));
		final int safeSup = min(safeInc(safeSum(index, sup)), partArray.length);

		return Utils.getStream(components)
			.map(splitting((oldC, newC) -> {  
				// stream the array slice containing all the datapoints in current window
				Stream<DataPoint> window = Stream.empty();
				if (index + inf < partArray.length && index + sup >= 0)
				{
					window = Arrays.stream(partArray, safeInf, safeSup);
					if (!Utils.SEQUENTIAL)
						window = window.parallel();
				}
				else
					window = Stream.empty();
				
				// Compute the analytic invocation over current window and measure and pair the result with the old value
				Collection<ScalarValue<?, ?, ?, ?>> result = window.collect(collectingAndThen(mapping(dp -> dp.get(oldC), collectors.get(oldC)), 
					v -> finishers.get(oldC).apply(v, partArray[index].get(oldC))));
				
				LOGGER.trace("Result for analytic invocation on component {} for datapoint {}:", newC, partArray[index]);
				LOGGER.trace("   -->: {}", result);
				return new SimpleEntry<>(newC, result);
			})).collect(collectingAndThen(entriesToMap(), toEntryWithValue(key -> partArray[index])));
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