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
import static it.bancaditalia.oss.vtl.impl.transform.util.LimitClause.CURRENT_DATA_POINT;
import static it.bancaditalia.oss.vtl.impl.transform.util.LimitClause.following;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.DATAPOINTS;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentSet;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Collections.emptyMap;
import static java.util.Collections.newSetFromMap;

import java.time.LocalDate;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.transform.util.SortClause;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
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
	
	private static class ListOfDateValues extends ArrayList<DateValue<?>>
	{
		private static final long serialVersionUID = 1L;
	}

	private final FillMode mode;

	public FillTimeSeriesTransformation(Transformation operand, FillMode mode)
	{
		super(operand);
		this.mode = mode == null ? ALL : mode;
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet ds, VTLValueMetadata metadata)
	{
		DataSetMetadata structure = ds.getMetadata();
		DataStructureComponent<?, ?, ?> timeID = ds.getMetadata().getComponents(Identifier.class, TIMEDS).iterator().next();
		Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(ds.getMetadata().getIDs());
		ids.remove(timeID);
		Map<DataStructureComponent<NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> nullFiller = 
				ds.getMetadata().getComponents(NonIdentifier.class).stream()
					.collect(toMapWithValues(c -> (ScalarValue<?, ?, ?, ?>) NullValue.instanceFrom(c)));
	
		if (mode == ALL)
		{
			Map<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<ScalarValue<?, ?, ?, ?>>> index;
			try (Stream<DataPoint> stream = ds.stream())
			{
				index = stream.collect(groupingByConcurrent(dp -> dp.getValues(ids), 
						mapping(dp -> (ScalarValue<?, ?, ?, ?>) dp.get(timeID), toConcurrentSet())));
			}

			Entry<Optional<? extends ScalarValue<?, ?, ?, ?>>, Optional<? extends ScalarValue<?, ?, ?, ?>>> e = Utils.getStream(index.values())
					.map(Utils::getStream)
					.collect(concatenating(true))
					.collect(teeing(
							minBy(DateValue.class, ScalarValue::compareTo), 
							maxBy(DateValue.class, ScalarValue::compareTo), 
						SimpleEntry::new));
			TimeValue<?, ?, ?, ?> min = (TimeValue<?, ?, ?, ?>) e.getKey().orElse(null);
			TimeValue<?, ?, ?, ?> max = (TimeValue<?, ?, ?, ?>) e.getValue().orElse(null);
			
			if (min == null || max == null)
				return ds;
			
			Set<TimeValue<?, ?, ?, ?>> times = new HashSet<>();
			for (TimeValue<?, ?, ?, ?> time = min; time.compareTo(max) <= 0; time = time.increment(1))
				times.add(time);
			
			Set<DataPoint> filled = newSetFromMap(new ConcurrentHashMap<>()); 
			for (Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> seriesKey: index.keySet())
			{
				Set<? extends ScalarValue<?, ?, ?, ?>> usedTimes = index.get(seriesKey);
				times.stream()
					.filter(not(usedTimes::contains))
					.map(time -> new DataPointBuilder(seriesKey)
							.add(timeID, time)
							.addAll(nullFiller)
							.build(LineageNode.of("filled"), structure)
					).forEach(filled::add);
			}
			
			return ds.union(dp -> LineageNode.of(this.toString(), dp.getLineage()), List.of(new AbstractDataSet(structure) {
				private static final long serialVersionUID = 1L;

				@Override
				protected Stream<DataPoint> streamDataPoints()
				{
					return Stream.concat(ds.stream(), filled.stream());
				}
			}));
		}
		else
		{
			// a function that fills holes between two dates (old is ignored)
			final SerBiFunction<List<DateValue<?>>, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>> timeFinisher = (pair, old) -> {
				if (pair.size() < 2)
					return List.of(pair.get(0));
				
				List<ScalarValue<?, ?, ?, ?>> result = new ArrayList<>();
				// TODO: Cast exception if not date
				LocalDate end = pair.get(1).get();
				// End-exclusive
				for (LocalDate start = pair.get(0).get(); start.compareTo(end) < 0; start = start.plus(1, DAYS))
					result.add(DateValue.of(start));
				
				return result;
			};

			// fill_time_series over partition by ids order by timeID between current datapoint and 1 following
			WindowClauseImpl windowClause = new WindowClauseImpl(ids, List.of(new SortClause(timeID, ASC)), 
					new WindowCriterionImpl(DATAPOINTS, CURRENT_DATA_POINT, following(1)));
			
			DataSetMetadata timeStructure = new DataStructureBuilder(structure.getIDs()).build();
			// Remove all measures and attributes then left-join the time-filled dataset with the old one
			
			return ds.mapKeepingKeys(timeStructure, DataPoint::getLineage, dp -> emptyMap())
					.analytic(dp -> LineageNode.of(this, dp.getLineage()), timeID, timeID, windowClause, null, 
							mapping(v -> (DateValue<?>) v, toList(ListOfDateValues::new)), timeFinisher)
					.mappedJoin(structure, ds, (a, b) -> coalesce(b, fill(a, structure)), true);
		}
	}

	private static DataPoint fill(DataPoint toBeFilled, DataSetMetadata structure)
	{
		return structure.stream()
			.filter(not(toBeFilled::containsKey))
			.map(Utils.toEntryWithValue(c -> (ScalarValue<?, ?, ?, ?>) NullValue.instanceFrom(c)))
			.collect(toDataPoint(toBeFilled.getLineage(), structure, toBeFilled));
	}
	
	@Override
	public String toString()
	{
		return "fill_time_series(" + operand + ", " + mode + ")";
	}

	@Override
	protected DataSetMetadata checkIsTimeSeriesDataSet(DataSetMetadata metadata, TransformationScheme scheme)
	{
		return metadata;
	}
}
