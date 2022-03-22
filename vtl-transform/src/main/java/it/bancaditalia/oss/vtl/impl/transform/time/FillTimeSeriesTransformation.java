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
import static it.bancaditalia.oss.vtl.model.data.DataPoint.compareBy;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.DATAPOINTS;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toCollection;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.util.SortClause;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.Utils;

public class FillTimeSeriesTransformation extends TimeSeriesTransformation
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(FillTimeSeriesTransformation.class);

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
	protected VTLValue evalOnDataset(DataSet ds, VTLValueMetadata metadata)
	{
		final DataSetMetadata structure = ds.getMetadata();
		final DataStructureComponent<Identifier, ?, ?> timeID = ds.getComponents(Identifier.class, TIMEDS).iterator().next();
		final Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(ds.getComponents(Identifier.class));
		ids.remove(timeID);
		final Map<DataStructureComponent<NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> nullFiller = ds.getComponents(NonIdentifier.class).stream()
				.collect(toMapWithValues(c -> (ScalarValue<?, ?, ?, ?>) NullValue.instanceFrom(c)));
	
		if (mode == ALL)
			try (Stream<DataPoint> stream = ds.stream())
			{
				AtomicReference<TimeValue<?, ?, ?, ?>> min = new AtomicReference<>();
				AtomicReference<TimeValue<?, ?, ?, ?>> max = new AtomicReference<>();
				stream.forEach(dp -> {
					min.accumulateAndGet((TimeValue<?, ?, ?, ?>) dp.get(timeID), (acc, tv) -> acc == null || tv.compareTo(acc) < 0 ? tv : acc);
					max.accumulateAndGet((TimeValue<?, ?, ?, ?>) dp.get(timeID), (acc, tv) -> acc == null || tv.compareTo(acc) > 0 ? tv : acc);
				});

				return new FunctionDataSet<>(structure, dataset -> {
					String alias = ds instanceof NamedDataSet ? ((NamedDataSet) ds).getAlias() : "Unnamed data set";
					LOGGER.debug("Filling time series for {}", alias);
					Stream<DataPoint> result = dataset.streamByKeys(ids, toCollection(() -> new ConcurrentSkipListSet<>(compareBy(timeID))), 
							seriesFiller(structure, timeID, nullFiller, min.get(), max.get()))
						.map(Utils::getStream)
						.collect(concatenating(Utils.ORDERED));
					LOGGER.debug("Finished filling time series for {}", alias);
					return result;
				}, ds);
			}
		else
		{
			// a function that fills holes between two dates (old is ignored)
			final SerBiFunction<List<ScalarValue<?, ?, ?, ?>>, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>> timeFinisher = (pair, old) -> {
				if (pair.size() == 1)
					return pair;
				
				List<ScalarValue<?, ?, ?, ?>> result = new ArrayList<>();
				// TODO: Cast exception if not date
				DateValue<?> end = (DateValue<?>) pair.get(1);
				// End-exclusive
				for (DateValue<?> start = (DateValue<?>) pair.get(0); start.compareTo(end) < 0; start = start.increment(1))
					result.add(start);
				
				return result;
			};

			// fill_time_series over partition by ids order by timeID between current datapoint and 1 following
			WindowClauseImpl windowClause = new WindowClauseImpl(ids, singletonList(new SortClause(timeID, ASC)), 
					new WindowCriterionImpl(DATAPOINTS, CURRENT_DATA_POINT, following(1L)));
			
			DataSetMetadata timeStructure = new DataStructureBuilder(structure.getComponents(Identifier.class)).build();
			// Remove all measures and attributes then left-join the time-filled dataset with the old one
			return ds.mapKeepingKeys(timeStructure, DataPoint::getLineage, dp -> emptyMap())
					.analytic(dp -> LineageNode.of(this, dp.getLineage()), timeID, windowClause, toList(), timeFinisher)
					.mappedJoin(structure, ds, (a, b) -> Utils.coalesce(b, fill(a, structure)), true);
		}
	}

	private static DataPoint fill(DataPoint toBeFilled, DataSetMetadata structure)
	{
		return Utils.getStream(structure)
			.filter(not(toBeFilled::containsKey))
			.map(Utils.toEntryWithValue(c -> (ScalarValue<?, ?, ?, ?>) NullValue.instanceFrom(c)))
			.collect(toDataPoint(toBeFilled.getLineage(), structure, toBeFilled));
	}

	/* Fills a single time series */
	private SerBiFunction<NavigableSet<DataPoint>, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, List<DataPoint>> seriesFiller(final DataSetMetadata structure,
			DataStructureComponent<Identifier, ?, ?> timeID, 
			Map<DataStructureComponent<NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> nullFilling, TimeValue<?, ?, ?, ?> min, TimeValue<?, ?, ?, ?> max)
	{
		return new SerBiFunction<NavigableSet<DataPoint>, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, List<DataPoint>>() 
		{
			private static final long serialVersionUID = 1L;

			@Override
			public List<DataPoint> apply(NavigableSet<DataPoint> series,
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> seriesID)
			{
				LOGGER.trace("Filling group {}", seriesID);
				List<DataPoint> additional = new LinkedList<>();
				
				// if min == null: do not add leading null datapoints (single mode)
				TimeValue<?, ?, ?, ?> previous = min != null ? min.increment(-1) : null; 
				
				// leading null elements and current elements
				for (DataPoint current: series)
				{
					TimeValue<?, ?, ?, ?> lastTime = (TimeValue<?, ?, ?, ?>) current.get(timeID);
					
					// find and fill holes
					if (previous != null)
					{
						TimeValue<?, ?, ?, ?> prevTime = previous.increment(1);
						while (lastTime.compareTo(prevTime) > 0)
						{
							LOGGER.trace("Filling space between {} and {}", prevTime, lastTime);
							DataPoint fillingDataPoint = new DataPointBuilder(seriesID)
								.add(timeID, prevTime)
								.addAll(nullFilling)
								.build(getLineage(), structure);
							additional.add(fillingDataPoint);
							prevTime = prevTime.increment(1);
						}
					}
		
					previous = (TimeValue<?, ?, ?, ?>) current.get(timeID);
				}
		
				// fill trailing holes
				if (mode == ALL && max != null)
				{
					TimeValue<?, ?, ?, ?> prevTime = previous;
					while (max.compareTo(prevTime) > 0)
					{
						LOGGER.trace("Filling space between {} and {}", prevTime, max);
						prevTime = prevTime.increment(1);
						DataPoint fillingDataPoint = new DataPointBuilder(seriesID)
							.add(timeID, prevTime)
							.addAll(nullFilling)
							.build(getLineage(), structure);
						additional.add(fillingDataPoint);
					}
				}
				
				additional.addAll(series);
				return additional;
			}
		};
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
