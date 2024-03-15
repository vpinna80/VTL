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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
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
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerCollectors;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public class DataPointBuilder implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataPointBuilder.class);

	public enum Option
	{
		DONT_SYNC;
	}
	
	private final Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> delegate;

	private volatile boolean built = false;

	public DataPointBuilder()
	{
		delegate = new ConcurrentHashMap<>();
	}

	public DataPointBuilder(Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keys, Option... options)
	{
		EnumSet<Option> optSet = EnumSet.noneOf(Option.class);
		for (Option option: options)
			optSet.add(option);
		
		delegate = optSet.contains(DONT_SYNC) ? new HashMap<>(keys) : new ConcurrentHashMap<>(keys);
	}

	private synchronized DataPointBuilder checkState()
	{
		if (built)
			throw new IllegalStateException("DataPoint already built.");
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
		if (!component.getVariable().getDomain().isAssignableFrom(value.getDomain()))
			throw new VTLCastException(component, value);
		
		final ScalarValue<?, ?, ?, ?> oldValue = delegate.putIfAbsent(component, value);

		if (value instanceof NullValue && oldValue != null && component.is(Identifier.class)) {
			throw new NullPointerException("Null value for identifier " + component);
		}
		
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
		Set<DataStructureComponent<?, ?, ?>> toDelete = Utils.getStream(delegate.keySet()).filter(c -> nameSet.contains(c.getVariable().getName())).collect(toSet());
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
		
		DataPoint dp = new DataPointImpl(requireNonNull(lineage), requireNonNull(structure, () -> "DataSet structure is null for " + delegate), delegate);
		LOGGER.trace("Datapoint@{} is {}", hashCode(), dp);
		return dp;
	}

	@Override
	public String toString()
	{
		return DataPointBuilder.class.getSimpleName() + delegate.toString();
	}

	private static class DataPointImpl extends AbstractMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> implements DataPoint, Serializable
	{
		private static final long serialVersionUID = 1L;
//		private static final Logger LOGGER = LoggerFactory.getLogger(DataPointImpl.class);

		private final Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dpValues;
		private final Lineage lineage;
		private final SerUnaryOperator<Lineage> enricher;
		private final int hashCode; 
		
		private transient Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> ids = null;

		private DataPointImpl(Lineage lineage, DataSetMetadata structure, Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> values)
		{
			this.lineage = lineage;
			this.dpValues = new HashMap<>(structure.size(), 1.0f);
			dpValues.putAll(values);

			if (!structure.equals(values.keySet()))
				for (DataStructureComponent<?, ?, ?> component: structure)
					if (!component.is(Identifier.class) && !values.containsKey(component))
						dpValues.put(component, NullValue.instanceFrom(component));

			if (!structure.equals(dpValues.keySet()))
			{
				this.dpValues.keySet()
					.stream()
					.filter(c -> !structure.contains(c))
					.findAny()
					.ifPresent(nonExistingComp -> {
						throw new VTLMissingComponentsException(nonExistingComp, structure);
					});

				Set<DataStructureComponent<?, ?, ?>> missing = new HashSet<>(structure);
				missing.removeAll(this.dpValues.keySet());
				if (missing.size() > 0)
					throw new VTLMissingComponentsException(missing, this.dpValues);
			}
			
			hashCode = dpValues.hashCode();
			enricher = SerUnaryOperator.identity();
		}

		private DataPointImpl(SerUnaryOperator<Lineage> enricher, DataPointImpl other)
		{
			this.lineage = other.lineage;
			this.enricher = other.enricher.andThen(enricher);
			this.dpValues = other.dpValues;
			hashCode = dpValues.hashCode();
		}

		@Override
		public <R extends Component> Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role)
		{
			if (role == Identifier.class)
			{
				if (ids == null)
					ids = Utils.getStream(dpValues).filter(entryByKey(k -> k.is(role))).map(keepingValue(k -> k.asRole(Identifier.class))).collect(SerCollectors.entriesToMap());
				// safe cast, R is Identifier
				@SuppressWarnings({ "unchecked", "rawtypes" })
				final Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> result = (Map) ids;
				return result;
			}
			else
				return Utils.getStream(dpValues.keySet()).filter(k -> k.is(role)).map(k -> k.asRole(role)).collect(SerCollectors.toMapWithValues(dpValues::get));
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
		public DataPoint combine(DataPoint other, SerBiFunction<DataPoint, DataPoint, Lineage> lineageCombiner)
		{
			Objects.requireNonNull(other);

			Set<String> thisComponentNames = keySet().stream().map(DataStructureComponent::getVariable).map(Variable::getName).collect(toSet());
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> finalMap = other.keySet().stream()
					.filter(c -> !thisComponentNames.contains(c.getVariable().getName()))
					.collect(toConcurrentMap(c -> c, other::get, (a, b) -> null, () -> new ConcurrentHashMap<>(this)));
			DataSetMetadata newStructure = new DataStructureBuilder(finalMap.keySet()).build();

			return new DataPointImpl(lineageCombiner.apply(this, other), newStructure, finalMap);
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
			if (dpValues.containsKey(key))
				return dpValues.get(key);
			else
				throw new VTLMissingComponentsException(Objects.toString(key), dpValues.keySet());
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
			return entrySet().stream()
				.map(Entry::toString)
				.collect(joining(", ", "{ ", " }"));
		}

		@Override
		public Lineage getLineage()
		{
			return enricher == SerUnaryOperator.<Lineage>identity() ? lineage : enricher.apply(lineage);
		}
		
		@Override
		public DataPoint enrichLineage(SerUnaryOperator<Lineage> enricher)
		{
			return new DataPointImpl(enricher, this);
		}
	}
}
