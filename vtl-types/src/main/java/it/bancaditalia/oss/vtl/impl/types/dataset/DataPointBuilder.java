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

import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerCollectors;
import it.bancaditalia.oss.vtl.util.Utils;

public class DataPointBuilder
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractDataSet.class);

	private final ConcurrentHashMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> delegate;

	private volatile boolean built = false;

	public DataPointBuilder()
	{
		delegate = new ConcurrentHashMap<>();
	}

	public DataPointBuilder(Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keys)
	{
		delegate = new ConcurrentHashMap<>(keys);
	}

	private synchronized DataPointBuilder checkState()
	{
		if (built)
			throw new IllegalStateException("DataPoint already built.");
		LOGGER.trace("Datapoint@{} is {}", hashCode(), delegate);
		return this;
	}

	public static <K extends DataStructureComponent<?, ?, ?>, V extends ScalarValue<?, ?, ?, ?>> SerCollector<? super Entry<? extends K, ? extends V>, DataPointBuilder, DataPoint> toDataPoint(
			Lineage lineage, DataSetMetadata structure, Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> startingValues)
	{
		return SerCollector.of(DataPointBuilder::new, DataPointBuilder::add, DataPointBuilder::merge, dpb -> dpb.addAll(startingValues).build(lineage, structure), EnumSet.of(CONCURRENT, UNORDERED));
	}

	public static <K extends DataStructureComponent<?, ?, ?>, V extends ScalarValue<?, ?, ?, ?>> SerCollector<? super Entry<? extends K, ? extends V>, DataPointBuilder, DataPoint> toDataPoint(
			Lineage lineage, DataSetMetadata structure)
	{
		return SerCollector.of(DataPointBuilder::new, DataPointBuilder::add, DataPointBuilder::merge, dpb -> dpb.build(lineage, structure), EnumSet.of(CONCURRENT, UNORDERED));
	}

	private static DataPointBuilder merge(DataPointBuilder left, DataPointBuilder right)
	{
		right.delegate.forEach(left::add);
		return left.checkState();
	}
	
	public DataPointBuilder addAll(Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> values)
	{
		values.forEach(this::add);
		return checkState();
	}

	public DataPointBuilder add(Entry<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> value)
	{
		return add(value.getKey(), value.getValue());
	}

	public DataPointBuilder add(DataStructureComponent<?, ?, ?> component, ScalarValue<?, ?, ?, ?> value)
	{
		if (!component.getDomain().isAssignableFrom(value.getDomain()))
			throw new VTLCastException(component.getDomain(), value);
		if (component.is(Identifier.class) && value instanceof NullValue)
			throw new NullPointerException("Null value for identifier " + component);
		
		final ScalarValue<?, ?, ?, ?> oldValue = delegate.putIfAbsent(component, value);
		
		if (oldValue != null && !oldValue.equals(value))
			throw new IllegalStateException("Different values for " + component + ": " + value.toString() + ", " + oldValue.toString());
		return checkState();
	}

	public DataPointBuilder delete(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		delegate.keySet().removeAll(components);
		return checkState();
	}

	public DataPointBuilder delete(String... names)
	{
		Set<String> nameSet = Utils.getStream(names).collect(toSet());
		Set<DataStructureComponent<?, ?, ?>> toDelete = Utils.getStream(delegate.keySet()).filter(c -> nameSet.contains(c.getName())).collect(toSet());
		delegate.keySet().removeAll(toDelete);
		return checkState();
	}

	public DataPointBuilder delete(DataStructureComponent<?, ?, ?>... components)
	{
		delegate.keySet().removeAll(Arrays.asList(components));
		return checkState();
	}

	public DataPoint build(Lineage lineage, DataSetMetadata structure)
	{
		if (built)
			throw new IllegalStateException("DataPoint already built");
		built = true;
		return new DataPointImpl(requireNonNull(lineage), requireNonNull(structure, () -> "DataSet structure is null for " + delegate), delegate);
	}

	@Override
	public String toString()
	{
		return DataPointBuilder.class.getSimpleName() + delegate.toString();
	}

	private static class DataPointImpl extends AbstractMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> implements DataPoint, Serializable
	{
		private static final long serialVersionUID = 1L;
		private static final Logger LOGGER = LoggerFactory.getLogger(DataPointImpl.class);

		private final Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dpValues;
		private final Lineage lineage;
		private final int hashCode; 

		private transient Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> ids = null;

		private DataPointImpl(Lineage lineage, DataSetMetadata structure, Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> values)
		{
			this.lineage = lineage;
			if (!(values instanceof Serializable))
				throw new IllegalStateException("The values map must be serializable");

			if (values.size() != structure.size())
			{
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> filledValues = new HashMap<>(values);
				for (DataStructureComponent<?, ?, ?> component: structure)
					if (!component.is(Identifier.class) && !values.containsKey(component))
						filledValues.put(component, NullValue.instanceFrom(component));
				this.dpValues = filledValues;
			} else
				this.dpValues = values;

			if (!structure.equals(dpValues.keySet()))
			{
				this.dpValues.keySet()
					.stream()
					.filter(c -> !structure.contains(c))
					.findAny()
					.ifPresent(nonExistingComp -> {
						IllegalStateException e = new IllegalStateException("Component " + nonExistingComp + " has a value but is not defined on " + structure);
						LOGGER.error("Component {} has a value but is not defined on {} in datapoint {}", nonExistingComp, structure, values, e);
						throw e;
					});

				Set<DataStructureComponent<?, ?, ?>> missing = new HashSet<>(structure);
				missing.removeAll(this.dpValues.keySet());
				if (missing.size() > 0)
					throw new VTLMissingComponentsException(missing, this.dpValues);
			}
			
			hashCode = dpValues.hashCode();
		}

		@Override
		public <R extends ComponentRole> Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role)
		{
			if (role == Identifier.class)
			{
				if (ids == null)
					ids = Utils.getStream(dpValues).filter(entryByKey(k -> k.is(role))).map(keepingValue(k -> k.as(Identifier.class))).collect(SerCollectors.entriesToMap());
				// safe cast, R is Identifier
				@SuppressWarnings({ "unchecked", "rawtypes" })
				final Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> result = (Map) ids;
				return result;
			}
			else
				return Utils.getStream(dpValues.keySet()).filter(k -> k.is(role)).map(k -> k.as(role)).collect(SerCollectors.toMapWithValues(dpValues::get));
		}

		@Override
		public DataPoint dropComponents(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> components)
		{
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> newVals = new HashMap<>(this);
			for (DataStructureComponent<?, ?, ?> component : components)
				if (!component.is(Identifier.class))
					newVals.remove(component);

			return new DataPointImpl(getLineage(), new DataStructureBuilder(newVals.keySet()).build(), newVals);
		}

		@Override
		public DataPoint combine(Transformation transformation, DataPoint other)
		{
			Objects.requireNonNull(other);

			Set<String> thisComponentNames = keySet().stream().map(DataStructureComponent::getName).collect(toSet());
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> finalMap = other.keySet().stream()
					.filter(c -> !thisComponentNames.contains(c.getName()))
					.collect(toConcurrentMap(c -> c, other::get, (a, b) -> null, () -> new ConcurrentHashMap<>(this)));
			DataSetMetadata newStructure = new DataStructureBuilder(finalMap.keySet()).build();

			return new DataPointImpl(LineageNode.of(transformation, getLineage(), other.getLineage()), newStructure, finalMap);
		}

		@Override
		public DataPoint keep(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> components) throws VTLMissingComponentsException
		{
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> oper = new HashMap<>(getValues(Identifier.class));

			for (DataStructureComponent<?, ?, ?> component : components)
				if (containsKey(component))
					oper.put(component, get(component));
				else
					throw new VTLMissingComponentsException(component, keySet());

			return new DataPointImpl(getLineage() ,new DataStructureBuilder(oper.keySet()).build(), oper);
		}

		@Override
		public DataPoint renameComponent(DataStructureComponent<?, ?, ?> oldComponent, DataStructureComponent<?, ?, ?> newComponent)
		{
			if (!containsKey(oldComponent))
				throw new VTLMissingComponentsException(oldComponent, keySet());

			if (newComponent == null)
				throw new VTLException("rename: new omponent cannot be null");

			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> newValues = new HashMap<>(this);
			ScalarValue<?, ?, ?, ?> value = newValues.remove(oldComponent);
			newValues.put(newComponent, value);
			return new DataPointImpl(getLineage(), new DataStructureBuilder(newValues.keySet()).build(), newValues);
		}

		@Override
		public boolean containsKey(Object key)
		{
			return dpValues.containsKey(key);
		}

		@Override
		public boolean containsValue(Object value)
		{
			return dpValues.containsValue(value);
		}

		@Override
		public Set<Map.Entry<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> entrySet()
		{
			return dpValues.entrySet();
		}

		@Override
		public boolean equals(Object o)
		{
			return dpValues.equals(o);
		}

		@Override
		public ScalarValue<?, ?, ?, ?> get(Object key)
		{
			return dpValues.get(key);
		}

		@Override
		public int hashCode()
		{
			return hashCode;
		}

		@Override
		public int size()
		{
			return dpValues.size();
		}

		@Override
		public String toString()
		{
			return entrySet().stream().peek(e -> Objects.nonNull(e.getKey())).peek(e -> Objects.nonNull(e.getValue())).map(Entry::toString)
					.collect(joining(", ", "{ ", " }"));
		}

		@Override
		public Lineage getLineage()
		{
			return lineage;
		}
	}
}
