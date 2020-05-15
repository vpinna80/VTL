/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.types.dataset;

import static java.util.stream.Collectors.groupingByConcurrent;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.Utils;

public class LightDataSet extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;
	private final Supplier<? extends Stream<DataPoint>> supplier;

	public LightDataSet(VTLDataSetMetadata dataStructure, Supplier<? extends Stream<DataPoint>> datapoints)
	{
		super(dataStructure);
		this.supplier = datapoints;
	}

	@Override
	public Stream<DataPoint> getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> idsToMatch = keyValues.keySet();
		return stream().filter(dp -> keyValues.equals(dp.getValues(idsToMatch, Identifier.class)));
	}

	@Override
	public <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter,
			BiFunction<? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, ? super Stream<DataPoint>, T> groupMapper)
	{
		Set<Entry<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>> entrySet = filter.entrySet();
		Set<DataStructureComponent<Identifier, ?, ?>> keySet = filter.keySet();
		
		ConcurrentMap<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, List<DataPoint>> map;
		try (Stream<DataPoint> stream = stream())
		{
			map = stream.filter(dp -> entrySet.containsAll(dp.getValues(keySet, Identifier.class).entrySet()))
					.collect(groupingByConcurrent(dp -> dp.getValues(keys, Identifier.class)
//							, Collector.of(Stream::<DataPoint>builder, Builder::add, 
//									(a, b) -> { b.build().forEach(a::add); return a; }, Builder::build, CONCURRENT, UNORDERED)
							));
		}
		
		return Utils.getStream(map.entrySet())
			.map(e -> groupMapper.apply(e.getKey(), Utils.getStream(e.getValue())));
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return supplier.get();
	}
}