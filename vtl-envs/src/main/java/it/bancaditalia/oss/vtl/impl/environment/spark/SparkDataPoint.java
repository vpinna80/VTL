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

import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.mergeError;
import static it.bancaditalia.oss.vtl.util.Utils.toMapWithValues;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.Utils;

public class SparkDataPoint extends AbstractMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> implements DataPoint, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final Lineage lineage;
	final Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> values;

	public static SparkDataPoint of(DataPoint other)
	{
		if (other instanceof SparkDataPoint)
			return (SparkDataPoint) other;
		else
			return new SparkDataPoint(other.getLineage(), other);
	}

	public SparkDataPoint(Lineage lineage, Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> values)
	{
		this.lineage = lineage;
		this.values = values;
	}

	public boolean isEmpty()
	{
		return values.isEmpty();
	}

	public boolean containsKey(Object key)
	{
		return values.containsKey(key);
	}

	public boolean containsValue(Object value)
	{
		return values.containsValue(value);
	}

	public ScalarValue<?, ?, ?, ?> get(Object key)
	{
		return values.get(key);
	}

	public Set<Entry<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> entrySet()
	{
		return unmodifiableMap(values).entrySet();
	}

	public Lineage getLineage()
	{
		return lineage;
	}

	@Override
	public DataPoint dropComponents(
			Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> components)
	{
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> kept = Utils.getStream(values)
				.filter(entryByKey(c -> c.is(Identifier.class) || !components.contains(c)))
				.collect(toMap(Entry::getKey, Entry::getValue, mergeError(), HashMap::new));
	
		return new SparkDataPoint(lineage, kept);
	}

	@Override
	public DataPoint keep(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> components)
	{
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> kept = Utils.getStream(values)
					.filter(entryByKey(c -> c.is(Identifier.class) || components.contains(c)))
					.collect(toMap(Entry::getKey, Entry::getValue, mergeError(), HashMap::new));
		
		return new SparkDataPoint(lineage, kept);
	}

	@Override
	public DataPoint renameComponent(DataStructureComponent<?, ?, ?> oldComponent,
			DataStructureComponent<?, ?, ?> newComponent)
	{
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> newValues = new HashMap<>(this);
		ScalarValue<?, ?, ?, ?> value = newValues.remove(oldComponent);
		newValues.put(newComponent, value);
		return new SparkDataPoint(getLineage(), newValues);
	}

	@Override
	public DataPoint combine(Transformation transformation, DataPoint other)
	{
		throw new UnsupportedOperationException("SparkDataPoint combine operation not implemented.");
	}

	@Override
	public <R extends ComponentRole> Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(
			Class<R> role)
	{
		return Utils.getStream(values.keySet())
				.filter(k -> k.is(role))
				.map(k -> k.as(role))
				.collect(toMapWithValues(values::get));
	}
}
