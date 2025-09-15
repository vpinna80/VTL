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
import static it.bancaditalia.oss.vtl.impl.transform.util.LimitClause.preceding;
import static it.bancaditalia.oss.vtl.impl.transform.util.SortClause.asc;
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl.DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING;
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl.dataPointsBetween;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.STRING_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.stream.Collector.Characteristics.CONCURRENT;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue.FillTimeSeriesHolder;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue.FillTimeSeriesTimeList;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeWithFreq;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDateDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimeDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class FillTimeSeriesTransformation extends TimeSeriesTransformation
{
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(FillTimeSeriesTransformation.class);
	
	private static final DataSetComponent<?, ?, ?> FREQ_COMP = DataSetComponentImpl.of(VTLAliasImpl.of(true, "$$FREQ$$"), DURATIONDS, Identifier.class);
	private static final DataSetComponent<Identifier, ?, ?> CLASS_COMP = DataSetComponentImpl.of(VTLAliasImpl.of(true, "$$CLASS$$"), STRINGDS, Identifier.class);
	private static final DataSetComponent<Measure, ?, ?> MIN_DATE = DataSetComponentImpl.of(VTLAliasImpl.of(true, "$$MIN_DATE$$"), DATEDS, Measure.class);
	private static final DataSetComponent<Measure, ?, ?> MAX_DATE = DataSetComponentImpl.of(VTLAliasImpl.of(true, "$$MAX_DATE$$"), DATEDS, Measure.class);
	private static final DataSetComponent<Measure, ?, ?> END_TIME = DataSetComponentImpl.of(VTLAliasImpl.of(true, "$$END_TIME$$"), TIMEDS, Measure.class);

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
	protected VTLValue evalOnDataset(TransformationScheme scheme, DataSet ds, VTLValueMetadata resultMetadata)
	{
		DataSetStructure structure = ds.getMetadata();
		DataSetComponent<Identifier, EntireTimeDomainSubset, TimeDomain> timeID = ds.getMetadata().getComponents(Identifier.class, TIMEDS).iterator().next();
		LineageNode lineageForFilled = LineageNode.of(this);
		Set<DataSetComponent<Identifier, ?, ?>> nonTimeIDs = new HashSet<>(ds.getMetadata().getIDs());
		nonTimeIDs.remove(timeID);

		// Used by spark to determine encoders
		ScalarValue<?, ?, ?, ?> sparkDefault = DateValue.of(LocalDate.now());		

		// First step: separate time values by holder class name
		DataSetStructure idsWithClass = new DataSetStructureBuilder(structure.getIDs()).addComponent(CLASS_COMP).build();
		DataSet result = ds.mapKeepingKeys(idsWithClass, identity(), 
			dp -> Map.of(CLASS_COMP, StringValue.of(dp.get(timeID).get().getClass().getSimpleName())));

		// Second step: Associate each ts value with that ts freq
		Set<DataSetComponent<Identifier, ?, ?>> classNonTimeIDs = new HashSet<>(nonTimeIDs);
		classNonTimeIDs.add(CLASS_COMP);
		WindowClause clause = new WindowClauseImpl(classNonTimeIDs, List.of(asc(timeID)), DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING);
		SerCollector<TimeValue<?, ?, ?, ?>, ?, DurationValue> collector = SerCollector.of(TimeWithFreq::new, TimeWithFreq::setTime, TimeWithFreq::combine, TimeWithFreq::getDuration, Set.of());
		result = result.analytic(identity(), timeID, FREQ_COMP, clause, null, collector, (freq, dp) -> Set.of(freq));

		if (mode == ALL)
		{
			// For "ALL" mode, we also need min(timeID) and max(timeID) across all time series
			
			// analytics to attach global min-max dates to each dts value
			// for globals, do not partition 
			clause = new WindowClauseImpl(Set.of(), List.of(), DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING);
			result = result.analytic(identity(), timeID, MIN_DATE, clause, null, 
				collectingAndThen(mapping(v -> ((TimeValue<?, ?, ?, ?>) v).getStartDate(), minBy(DateValue.class, DateValue::compareTo)), o -> o.isEmpty() ? sparkDefault : o.get()), (a, b) -> Set.of(a));
			clause = new WindowClauseImpl(Set.of(), List.of(), DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING);
			result = result.analytic(identity(), timeID, MAX_DATE, clause, null, 
				collectingAndThen(mapping(v -> ((TimeValue<?, ?, ?, ?>) v).getEndDate(), maxBy(DateValue.class, DateValue::compareTo)), o -> o.isEmpty() ? sparkDefault : o.get()), (a, b) -> Set.of(a));
		}
		else
		{
			// For "SINGLE" mode just add null for min/max dates
			DataSetStructure withMinMax = new DataSetStructureBuilder(idsWithClass).addComponents(FREQ_COMP, MIN_DATE, MAX_DATE).build();
			Map<DataSetComponent<Measure, ? extends ValueDomainSubset<?, ? extends ValueDomain>, ? extends ValueDomain>, NullValue<EntireDateDomainSubset, DateDomain>> nullMinMax = Map.of(MIN_DATE, NullValue.instance(DATEDS), MAX_DATE, NullValue.instance(DATEDS));
			result = result.mapKeepingKeys(withMinMax, identity(), dp -> nullMinMax);
		}

		// Third step: analytics to attach end time for each ts
		Set<DataSetComponent<Identifier, ?, ?>> classFreqNonTimeIDs = new HashSet<>(classNonTimeIDs);
		classFreqNonTimeIDs.add(CLASS_COMP);
		clause = new WindowClauseImpl(classFreqNonTimeIDs, List.of(), DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING);

		result = result.analytic(identity(), timeID, END_TIME, clause, null, collectingAndThen(maxBy(TimeValue.class, 
			(a, b) -> ((ScalarValue<?, ?, ?, ?>) a).compareTo((ScalarValue<?, ?, ?, ?>) b)), o -> o.isEmpty() ? sparkDefault : o.get()), (a, b) -> Set.of((TimeValue<?, ?, ?, ?>) a));
		
		// Fourth step: for each ts, fill holes in each pair of adjacent times
		// over (partiton by (all nonTimeIDs) order by timeID data points between 1 preceding and current data point)
		clause = new WindowClauseImpl(classFreqNonTimeIDs, List.of(asc(timeID)), dataPointsBetween(preceding(1), CURRENT_DATA_POINT));
		// Collector that fills a time interval interpolating times based on frequency
		SerCollector<DataPoint, ?, FillTimeSeriesTimeList> fillingCollector = SerCollector.of(
			() -> new FillTimeSeriesHolder(),
			(acc, dp) -> {
				acc.setConstants((DurationValue) dp.get(FREQ_COMP), () -> dp.get(MIN_DATE), () -> dp.get(MAX_DATE), () -> dp.get(END_TIME));
				acc.add((TimeValue<?, ?, ?, ?>) dp.get(timeID));
			},
			(a, b) -> { throw new IllegalStateException("Merging not allowed"); },
			acc -> fillTimes(acc),
			EnumSet.of(CONCURRENT)
		);
		result = result.analytic(identity(), timeID, timeID, clause, identity(), fillingCollector, (ftstl, b) -> ftstl.getList());
		
		// Fifth step: Add a dummy measure and remove min-max-start-end
		DataSetStructure withDummy = new DataSetStructureBuilder(idsWithClass).addComponents(STRING_VAR, FREQ_COMP).build();
		result = result.mapKeepingKeys(withDummy, identity(), dp -> Map.of(STRING_VAR, NullValue.instance(STRINGDS)));
		
		// Sixth step: remove class & freq using a do-nothing collector
		SerCollector<DataPoint, Object, Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> doNothing = SerCollector.of(
			() -> null, 
			(a, b) -> {}, 
			(a, b) -> a,
			a -> Map.of(),
			EnumSet.of(CONCURRENT)
		);
		result = result.aggregate(DataSet.class, structure, structure.getIDs(), doNothing, (map, lineages, keyValues) -> {
			return getFilled(keyValues, structure, lineageForFilled);
		});
		
		// Seventh step: perform a union
		return ds.union(List.of(result), identity());
	}
	
	// wrapper to massage parameters
	@SuppressWarnings("unchecked")
	private <T extends TimeValue<?, ?, ?, ?>> FillTimeSeriesTimeList fillTimes(FillTimeSeriesHolder toBeFilled)
	{
		// If the "inf" date is null, we're in the process of spark determining the encoder to use, just return an empty list.
		if (toBeFilled.get(3) == null)
			return new FillTimeSeriesTimeList().setList(List.of());
		
		if (toBeFilled.get(0) != null && toBeFilled.get(0).isNull())
			throw new IllegalStateException();
		
		return fillTimes(toBeFilled.getDuration().get(), 
			(T) toBeFilled.get(3), 
			(T) toBeFilled.get(4), 
			(DateValue<?>) toBeFilled.get(0), 
			(DateValue<?>) toBeFilled.get(1),
			(TimeValue<?, ?, ?, ?>) toBeFilled.get(2));
	}

	// Fill any two consecutive times in a single time series
	// Also, if ALL mode, chech if we're at start or end and fill extremities
	private static FillTimeSeriesTimeList fillTimes(Frequency frequency, TimeValue<?, ?, ?, ?> inf, 
		TimeValue<?, ?, ?, ?> sup, DateValue<?> minDate, DateValue<?> maxDate, TimeValue<?, ?, ?, ?> tsEnd)
	{
		List<TimeValue<?, ?, ?, ?>> result = new ArrayList<>();
		
		// if ALL mode and we're at the start, fill times from min to the start
		if (sup == null && minDate != null)
			for (TimeValue<?, ?, ?, ?> start = ((TimeValue<?, ?, ?, ?>) inf).minus(frequency.getPeriod());
					minDate.get().equals(start.getStartDate().get()) || minDate.get().isBefore(start.getStartDate().get());
					start = start.minus(frequency.getPeriod()))
				result.add(start);
		
		// if there are two times, fill any holes between them
		TimeValue<?, ?, ?, ?> current = ((TimeValue<?, ?, ?, ?>) inf).add(frequency.getPeriod());
		if (sup != null)
			while (current.compareTo(sup) < 0)
			{
				result.add(current);
				current = current.add(frequency.getPeriod());
			}
		
		// if ALL mode and we're at the end, fill times from the end to max
		// Handle both ts with just 1 time as well as any other series with multiple times 
		TimeValue<?, ?, ?, ?> end = sup != null ? sup : inf;
		if (maxDate != null && end.equals(tsEnd))
			for (end = end.add(frequency.getPeriod());
					maxDate.get().equals(end.getEndDate().get()) || maxDate.get().isAfter(end.getEndDate().get()); 
					end = end.add(frequency.getPeriod()))
				result.add(end);
		
		return new FillTimeSeriesTimeList().setList(result);
	}

	private static DataPoint getFilled(Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> map, DataSetStructure structure, Lineage lineage)
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
