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
package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode.ALL;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithKeys;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.getStream;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.Collections.sort;
import static java.util.stream.Stream.concat;

import java.time.Period;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimeDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class FillTimeSeriesTransformation extends TimeSeriesTransformation
{
	private static final long serialVersionUID = 1L;

	public enum FillMode
	{
		ALL("all"), SINGLE("single");

		private String name;

		private FillMode(String name)
		{
			this.name = name;
		}
		
		@Override
		public String toString()
		{
			return name;
		}
	}

	private final FillMode mode;

	public FillTimeSeriesTransformation(Transformation operand, FillMode mode)
	{
		super(operand);
		this.mode = mode == null ? ALL : mode;
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet ds, VTLValueMetadata metadata, TransformationScheme scheme)
	{
		DataSetStructure structure = ds.getMetadata();
		DataSetComponent<Identifier, EntireTimeDomainSubset, TimeDomain> timeID = ds.getMetadata().getComponents(Identifier.class, TIMEDS).iterator().next();
		Set<DataSetComponent<Identifier, ?, ?>> nonTimeIDs = new HashSet<>(ds.getMetadata().getIDs());
		nonTimeIDs.remove(timeID);

		LineageNode filledLineage = LineageNode.of(this);
	
		if (mode == ALL)
		{
			// Index cannot be cached unless early-evaluated.
			// This will materialize all the datapoints into the Spark driver (cfr. SparkDataSet).
			Map<Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, ConcurrentMap<Class<?>, ConcurrentMap<TimeValue<?, ?, ?, ?>, DataPoint>>> index;
			try (Stream<DataPoint> stream = ds.stream())
			{
				index = stream.collect(groupingByConcurrent(dp -> dp.getValues(nonTimeIDs), 
						groupingByConcurrent(dp -> dp.getValue(timeID).get().getClass(),
								toMapWithKeys(dp -> (TimeValue<?, ?, ?, ?>) dp.getValue(timeID)))));
			}

			// Group times of all series by frequency and for each compute a max and a min for the corresponding set of times
			List<Entry<TimeValue<?, ?, ?, ?>, TimeValue<?, ?, ?, ?>>> minMaxes = Utils.getStream(index.values())
				.map(m -> m.values().stream())
				.collect(concatenating(ORDERED))
				.map(Map::keySet)
				.map(Utils::getStream)
				.collect(concatenating(ORDERED))
				// at this point this is Stream<TimeValue<?, ?, ?, ?>> and the max/min 
				// can be computed (for each frequency/class) with the two collectors
				.collect(collectingAndThen(groupingByConcurrent(TimeValue::getFrequency, 
						mapping(v -> (TimeValue<?, ?, ?, ?>) v, teeing(
							collectingAndThen(minBy(TimeValue.class, TimeValue::compareTo), Optional::get), 
							collectingAndThen(maxBy(TimeValue.class, TimeValue::compareTo), Optional::get), 
							SimpleEntry<TimeValue<?, ?, ?, ?>, TimeValue<?, ?, ?, ?>>::new))),
						map -> {
							// Sort the entries from largest to smallest frequency (of the min part only)
							List<Entry<TimeValue<?, ?, ?, ?>, TimeValue<?, ?, ?, ?>>> sortedValues = new ArrayList<>(map.values());
							sort(sortedValues, (e1, e2) -> e1.getKey().getFrequency().compareTo(e2.getKey().getFrequency()));
							return sortedValues;
						})
				);
			
			TimeValue<?, ?, ?, ?> min = null, max = null;
			
			// determine absolute min/max across all frequencies
			for (Entry<TimeValue<?, ?, ?, ?>, TimeValue<?, ?, ?, ?>> entry: minMaxes)
			{
				if (min == null)
					min = entry.getKey();
				if (max == null)
					max = entry.getValue();
				
				if (entry.getKey().getStartDate().compareTo(min.getStartDate()) < 0)
					min = entry.getKey();
				if (entry.getValue().getEndDate().compareTo(max.getEndDate()) > 0)
					max = entry.getValue();
			}
			
			// convert absolute min/max to a map from frequencies to min/max-es
			TimeValue<?, ?, ?, ?> finalMin = min, finalMax = max;
			Map<DurationValue, Entry<TimeValue<?, ?, ?, ?>, TimeValue<?, ?, ?, ?>>> minMaxMap = minMaxes.stream()
				.map(splitting((smin, smax) -> {
					while(smin.getStartDate().compareTo(finalMin.getStartDate()) > 0)
						smin = smin.add(-1);
					while(smax.getEndDate().compareTo(finalMax.getEndDate()) < 0)
						smax = smax.add(1);
					return new SimpleEntry<TimeValue<?, ?, ?, ?>, TimeValue<?, ?, ?, ?>>(smin, smax);
				})).collect(toMap(e -> e.getKey().getFrequency(), identity()));
			
			return new FunctionDataSet<>(structure, idx -> getStream(idx.entrySet())
				.map(splitting((dpTemplate, seriesGroup) -> Utils.getStream(seriesGroup.values())
					.map(series -> {
						DurationValue freq = series.keySet().iterator().next().getFrequency();
						return fillSingleTimeSeries(structure, timeID, dpTemplate, series, filledLineage, 
								minMaxMap.get(freq).getKey(), minMaxMap.get(freq).getValue());
					}).collect(concatenating(ORDERED))
				)).collect(concatenating(ORDERED)), index);
		}
		else
		{
			// Index cannot be cached unless early-evaluated.
			// This will materialize all the datapoints into the Spark driver (cfr. SparkDataSet).
			Map<Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, ConcurrentMap<Class<?>, ConcurrentMap<TimeValue<?, ?, ?, ?>, DataPoint>>> index;
			try (Stream<DataPoint> stream = ds.stream())
			{
				index = stream.collect(groupingByConcurrent(dp -> dp.getValues(nonTimeIDs), 
						groupingByConcurrent(dp -> dp.getValue(timeID).get().getClass(),
								toMapWithKeys(dp -> (TimeValue<?, ?, ?, ?>) dp.getValue(timeID)))));
			}
			
			return new FunctionDataSet<>(structure, idx -> getStream(idx.entrySet())
				// Grouping by the effective time holder class should effectively group by differing frequencies
				.map(splitting((dpTemplate, seriesGroup) -> Utils.getStream(seriesGroup.values())
					.map(series -> fillSingleTimeSeries(structure, timeID, dpTemplate, series, filledLineage, null, null))
					.collect(concatenating(ORDERED))
				)).collect(concatenating(ORDERED)), index);
		}
	}

	private static Stream<DataPoint> fillSingleTimeSeries(DataSetStructure structure, DataSetComponent<?, ?, ?> timeID,
			Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dpTemplate, ConcurrentMap<TimeValue<?, ?, ?, ?>, DataPoint> series,
			Lineage filledLineage, TimeValue<?, ?, ?, ?> minTime, TimeValue<?, ?, ?, ?> maxTime)
	{
		Set<DataPoint> filled = new HashSet<>();
		TreeSet<TimeValue<?, ?, ?, ?>> timeSequence = new TreeSet<>(series.keySet());
		
		// Find the effective series frequency
		Iterator<TimeValue<?, ?, ?, ?>> iter = timeSequence.iterator();
		TimeValue<?, ?, ?, ?> last = iter.next();
		Frequency frequency = null;
		while (iter.hasNext())
		{
			TimeValue<?, ?, ?, ?> current = iter.next();
			Period p = last.until(current);
			for (Frequency testFrequency: Frequency.values())
				// Only allow inferred frequency to become smaller (i.e. from A to Q but not the opposite)
				if ((frequency == null || testFrequency.compareWith(frequency) <= 0) && testFrequency.isMultiple(p))
				{
					frequency = testFrequency;
					break;
				}
			last = current;
		}

		last = minTime;

		// if ALL, fill all periods from absolute min until the first period
		if (minTime != null)
			while (last != null && last.compareTo(timeSequence.first()) < 0)
			{
				Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dpTemplate);
				map.put(timeID, last);
				filled.add(getFilled(map, structure, filledLineage));
				last = last.add(frequency.getPeriod());
			}

		// Fill holes
		for (TimeValue<?, ?, ?, ?> current: timeSequence)
		{
			while (last != null && last.compareTo(current) < 0)
			{
				Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dpTemplate);
				map.put(timeID, last);
				filled.add(getFilled(map, structure, filledLineage));
				last = last.add(frequency.getPeriod());
			}
			
			last = current.add(frequency.getPeriod());
		}
		
		// if ALL, fill all periods until the absolute max
		if (maxTime != null)
			while (last != null && last.compareTo(maxTime) <= 0)
			{
				Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dpTemplate);
				map.put(timeID, last);
				filled.add(getFilled(map, structure, filledLineage));
				last = last.add(frequency.getPeriod());
			}
		
		return concat(getStream(filled), getStream(series.values()));
	}

	private static DataPoint getFilled(Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map, DataSetStructure structure, Lineage lineage)
	{
		// Sets all the missing measures and attributes in an filled-in datapoint to NULL  
		return structure.stream()
			.filter(not(map::containsKey))
			.map(toEntryWithValue(c -> (ScalarValue<?, ?, ?, ?>) NullValue.instanceFrom(c)))
			.collect(toDataPoint(lineage, structure, map));
	}
	
	@Override
	public String toString()
	{
		return "fill_time_series(" + operand + ", " + mode + ")";
	}

	@Override
	protected DataSetStructure checkIsTimeSeriesDataSet(DataSetStructure metadata, TransformationScheme scheme)
	{
		return metadata;
	}
}
