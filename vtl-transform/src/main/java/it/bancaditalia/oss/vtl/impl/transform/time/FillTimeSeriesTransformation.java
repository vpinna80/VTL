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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.util.Utils.toMapWithValues;
import static java.util.stream.Collectors.toCollection;

import java.util.Comparator;
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

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
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
	protected VTLValue evalOnDataset(DataSet ds)
	{
		final DataSetMetadata structure = ds.getMetadata();
		final DataStructureComponent<Identifier, ? extends TimeDomainSubset<?, ?>, TimeDomain> timeID = ds.getComponents(Identifier.class, TIMEDS).iterator().next();
		Set<DataStructureComponent<Identifier, ?, ?>> temp = new HashSet<>(ds.getComponents(Identifier.class));
		temp.remove(timeID);
		final Set<DataStructureComponent<Identifier, ?, ?>> ids = temp;
		final Map<DataStructureComponent<NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> nullFiller = ds.getComponents(NonIdentifier.class).stream()
				.collect(toMapWithValues(c -> (ScalarValue<?, ?, ?, ?>) NullValue.instanceFrom(c)));
		
		final Comparator<DataPoint> comparator = (dp1, dp2) -> {
			for (DataStructureComponent<Identifier, ?, ?> id: ids)
			{
				int c = dp1.get(id).compareTo(dp2.get(id));
				if (c != 0)
					return c;
			}
			
			return dp1.get(timeID).compareTo(dp2.get(timeID));
		};
		
		TimeValue<?, ?, ?, ?> min, max;
		if (mode == ALL && !ids.isEmpty())
			try (Stream<DataPoint> stream = ds.stream())
			{
				AtomicReference<TimeValue<?, ?, ?, ?>> minR = new AtomicReference<>();
				AtomicReference<TimeValue<?, ?, ?, ?>> maxR = new AtomicReference<>();
				stream.forEach(dp -> {
					minR.accumulateAndGet((TimeValue<?, ?, ?, ?>) dp.get(timeID), (acc, tv) -> acc == null || tv.compareTo(acc) < 0 ? tv : acc);
					maxR.accumulateAndGet((TimeValue<?, ?, ?, ?>) dp.get(timeID), (acc, tv) -> acc == null || tv.compareTo(acc) > 0 ? tv : acc);
				});
				
				min = minR.get();
				max = maxR.get();
			}
		else
		{
			min = null;
			max = null;
		}

		return new LightFDataSet<>(structure, dataset -> dataset.streamByKeys(ids, toCollection(() -> new ConcurrentSkipListSet<>(comparator)), 
					(elements, idValues) -> fillSeries(structure, elements, idValues, timeID, nullFiller, min, max))
				.reduce(Stream::concat)
				.orElse(Stream.empty()), ds);
	}

	private Stream<DataPoint> fillSeries(final DataSetMetadata structure, NavigableSet<DataPoint> series,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> seriesID, 
			DataStructureComponent<Identifier, ? extends TimeDomainSubset<?, ?>, TimeDomain> timeID, 
			Map<DataStructureComponent<NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> nullFilling, TimeValue<?, ?, ?, ?> min, TimeValue<?, ?, ?, ?> max)
	{
		LOGGER.debug("Filling group " + seriesID);
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
					LOGGER.debug("Filling space between " + prevTime + " and " + lastTime);
					DataPoint fillingDataPoint = new DataPointBuilder(seriesID)
						.add(timeID, prevTime)
						.addAll(nullFilling)
						.build(structure);
					additional.add(fillingDataPoint);
					prevTime = prevTime.increment(1);
				}
			}

			previous = (TimeValue<?, ?, ?, ?>) current.get(timeID);
		}

		// fill trailing holes
		if (mode == ALL)
		{
			TimeValue<?, ?, ?, ?> prevTime = previous;
			while (max.compareTo(prevTime) > 0)
			{
				LOGGER.debug("Filling space between " + prevTime + " and " + max);
				prevTime = prevTime.increment(1);
				DataPoint fillingDataPoint = new DataPointBuilder(seriesID)
					.add(timeID, prevTime)
					.addAll(nullFilling)
					.build(structure);
				additional.add(fillingDataPoint);
			}
		}
		
		return Stream.concat(Utils.getStream(series), Utils.getStream(additional));
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
