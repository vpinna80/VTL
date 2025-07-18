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
package it.bancaditalia.oss.vtl.impl.environment.spark;

import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.Utils.getStream;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class SparkDataPoint extends AbstractMap<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> implements DataPoint, Serializable 
{
	private static final long serialVersionUID = 1L;
	
	private final Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map;
	private final Lineage lineage;

	public SparkDataPoint(DataSetComponent<?, ?, ?>[] comps, ScalarValue<?, ?, ?, ?>[] vals, Lineage lineage)
	{
		map = new HashMap<>(comps.length);
		
		for (int i = 0; i < comps.length; i++)
			map.put(comps[i], vals[i]);
		this.lineage = lineage;
	}
	
	public SparkDataPoint(Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map, Lineage lineage)
	{
		this.map = new HashMap<>(map);
		this.lineage = lineage;
	}
	
	@Override
	public DataPoint drop(Collection<? extends DataSetComponent<? extends NonIdentifier, ?, ?>> toDrop)
	{
		SparkDataPoint dp = new SparkDataPoint(this, lineage);
		dp.map.keySet().removeAll(toDrop);
		return dp;
	}

	@Override
	public DataPoint keep(Collection<? extends DataSetComponent<? extends NonIdentifier, ?, ?>> toKeep)
	{
		SparkDataPoint dp = new SparkDataPoint(this, lineage);
		dp.map.keySet().retainAll(toKeep);
		return dp; 
	}

	@Override
	public DataPoint rename(DataSetComponent<?, ?, ?> oldComponent, DataSetComponent<?, ?, ?> newComponent)
	{
		SparkDataPoint dp = new SparkDataPoint(this, lineage);
		dp.map.put(newComponent, dp.map.remove(oldComponent));
		return dp;
	}

	@Override
	public DataPoint combine(DataPoint other, SerBiFunction<DataPoint, DataPoint, Lineage> lineageCombiner)
	{
		SparkDataPoint dp = new SparkDataPoint(other, lineage);
		dp.map.putAll(this);
		return dp;
	}

	@Override
	public <R extends Component> Map<DataSetComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role)
	{
		Map<DataSetComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> newMap = getStream(keySet())
			.filter(c -> c.is(role))
			.map(c -> c.asRole(role))
			.collect(toMapWithValues(this::get));
			
		return newMap;
	}

	@Override
	public Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Collection<? extends DataSetComponent<?, ?, ?>> toGet)
	{
		Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> newMap = getStream(keySet())
				.filter(toGet::contains)
				.collect(toMapWithValues(this::get));
		
		return newMap;
	}

	@Override
	public Lineage getLineage()
	{
		return lineage;
	}

	@Override
	public DataPoint enrichLineage(SerUnaryOperator<Lineage> enricher)
	{
		return new SparkDataPoint(this, enricher.apply(lineage));
	}

	@Override
	public int hashCode()
	{
		return map.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null || !(obj instanceof DataPoint))
			return false;

		return map.equals(obj);
	}
	
	@Override
	public int size()
	{
		return map.size();
	}

	@Override
	public boolean isEmpty()
	{
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key)
	{
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value)
	{
		return map.containsValue(value);
	}

	@Override
	public ScalarValue<?, ?, ?, ?> get(Object key)
	{
		return map.get(key);
	}

	@Override
	public Set<DataSetComponent<?, ?, ?>> keySet()
	{
		return unmodifiableSet(map.keySet());
	}

	@Override
	public Collection<ScalarValue<?, ?, ?, ?>> values()
	{
		return unmodifiableCollection(map.values());
	}

	@Override
	public Set<Entry<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> entrySet()
	{
		return unmodifiableSet(map.entrySet());
	}
}