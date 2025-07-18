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
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.lang.invoke.VarHandle;
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
import it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerCollectors;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public class DataPointBuilder implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataPointBuilder.class);
	private static final VarHandle IS_BUILT_HANDLE;

	public enum Option
	{
		DONT_SYNC;
	}
	
	static {
        try
        {
        	IS_BUILT_HANDLE = lookup().findVarHandle(DataPointBuilder.class, "isBuilt", boolean.class);
        }
        catch (ReflectiveOperationException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }
	
	private final Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> delegate;
	private boolean isBuilt = false;

	public DataPointBuilder(Option... options)
	{
		this(Map.of(), options);
	}

	public DataPointBuilder(Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keys, Option... options)
	{
		EnumSet<Option> optSet = EnumSet.noneOf(Option.class);
		for (Option option: options)
			optSet.add(option);
		
		delegate = optSet.contains(DONT_SYNC) ? new HashMap<>(keys) : new ConcurrentHashMap<>(keys);
	}

	private DataPointBuilder checkState()
	{
		if (isBuilt)
			throw new IllegalStateException("DataPoint already built.");
		return this;
	}
	
	public static DataPoint dpFromMap(Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map, DataSetStructure structure, Lineage lineage)
	{
		return new DataPointImpl(lineage, structure, map);
	}

	public static <K extends DataSetComponent<?, ?, ?>, V extends ScalarValue<?, ?, ?, ?>> SerCollector<? super Entry<? extends K, ? extends V>, DataPointBuilder, DataPoint> toDataPoint(
			Lineage lineage, DataSetStructure structure, 
			Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> startingValues)
	{
		return SerCollector.of(DataPointBuilder::new, DataPointBuilder::add, DataPointBuilder::merge, dpb -> dpb.addAll(startingValues).build(lineage, structure), EnumSet.of(CONCURRENT, UNORDERED));
	}

	public static <K extends DataSetComponent<?, ?, ?>, V extends ScalarValue<?, ?, ?, ?>> SerCollector<? super Entry<? extends K, ? extends V>, DataPointBuilder, DataPoint> toDataPoint(
			Lineage lineage, DataSetStructure structure)
	{
		return SerCollector.of(DataPointBuilder::new, DataPointBuilder::add, DataPointBuilder::merge, dpb -> dpb.build(lineage, structure), EnumSet.of(CONCURRENT, UNORDERED));
	}

	private static DataPointBuilder merge(DataPointBuilder left, DataPointBuilder right)
	{
		right.delegate.forEach(left::add);
		return left.checkState();
	}
	
	public DataPointBuilder addAll(Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> values)
	{
		values.forEach(this::add);
		return checkState();
	}

	public DataPointBuilder add(Entry<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> value)
	{
		return add(value.getKey(), value.getValue());
	}

	public DataPointBuilder add(CommonComponents component, ScalarValue<?, ?, ?, ?> value)
	{
		return add(component.get(), value);
	}
	
	public DataPointBuilder add(DataSetComponent<?, ?, ?> component, ScalarValue<?, ?, ?, ?> value)
	{
		if (!component.getDomain().isAssignableFrom(value.getDomain()))
			throw new VTLCastException(component, value);
		
		final ScalarValue<?, ?, ?, ?> oldValue = delegate.putIfAbsent(component, value);

		if (value.isNull() && oldValue != null && component.is(Identifier.class)) {
			throw new NullPointerException("Null value for identifier " + component);
		}
		
		if (oldValue != null && !oldValue.equals(value))
			throw new IllegalStateException("Different values for " + component + ": " + value.toString() + ", " + oldValue.toString());
		return checkState();
	}

	public DataPointBuilder delete(Collection<? extends DataSetComponent<?, ?, ?>> components)
	{
		delegate.keySet().removeAll(components);
		return checkState();
	}

	public DataPointBuilder delete(VTLAlias... names)
	{
		Set<VTLAlias> nameSet = Utils.getStream(names).collect(toSet());
		Set<DataSetComponent<?, ?, ?>> toDelete = Utils.getStream(delegate.keySet()).filter(c -> nameSet.contains(c.getAlias())).collect(toSet());
		delegate.keySet().removeAll(toDelete);
		return checkState();
	}

	public DataPointBuilder delete(DataSetComponent<?, ?, ?>... components)
	{
		delegate.keySet().removeAll(Arrays.asList(components));
		return checkState();
	}

	public DataPoint build(Lineage lineage, DataSetStructure structure)
	{
		if (!IS_BUILT_HANDLE.compareAndSet(this, false, true))
			throw new IllegalStateException("DataPoint already built.");
		
		DataPoint dp = new DataPointImpl(requireNonNull(lineage), requireNonNull(structure, () -> "DataSet structure is null for " + delegate), delegate);
		LOGGER.trace("Datapoint@{} is {}", hashCode(), dp);
		return dp;
	}

	@Override
	public String toString()
	{
		return DataPointBuilder.class.getSimpleName() + delegate.toString();
	}

	private static class DataPointImpl extends AbstractMap<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> implements DataPoint, Serializable
	{
		private static final long serialVersionUID = 1L;

		private final Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> values;
		private final Lineage lineage;
		private final SerUnaryOperator<Lineage> enricher;
		private final int hashCode; 
		
		private transient Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> ids = null;

		private DataPointImpl(Lineage lineage, DataSetStructure structure, Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> values)
		{
			this.lineage = lineage;
			values.putAll(values);

			for (DataSetComponent<?, ?, ?> c: structure)
				if (c.is(Attribute.class))
					values.putIfAbsent(c, NullValue.instanceFrom(c));

//			if (LOGGER.isTraceEnabled())
//			{
				values.keySet().stream()
					.filter(c -> !structure.contains(c))
					.findAny()
					.ifPresent(nonExistingComp -> {
						throw new VTLMissingComponentsException(structure, nonExistingComp);
					});
	
				Set<DataSetComponent<?, ?, ?>> missing = new HashSet<>(structure);
				missing.removeAll(values.keySet());
				if (missing.size() > 0)
					throw new VTLMissingComponentsException(values, missing);
//			}
			
			this.values = values;
			hashCode = values.hashCode();
			enricher = SerUnaryOperator.identity();
		}

		private DataPointImpl(SerUnaryOperator<Lineage> enricher, DataPointImpl other)
		{
			this.lineage = other.lineage;
			this.enricher = other.enricher.andThen(enricher);
			this.values = other.values;
			hashCode = values.hashCode();
		}

		@Override
		public <R extends Component> Map<DataSetComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role)
		{
			if (role == Identifier.class)
			{
				if (ids == null)
					ids = Utils.getStream(values).filter(entryByKey(k -> k.is(role))).map(keepingValue(k -> k.asRole(Identifier.class))).collect(SerCollectors.entriesToMap());
				// safe cast, R is Identifier
				@SuppressWarnings({ "unchecked", "rawtypes" })
				final Map<DataSetComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> result = (Map) ids;
				return result;
			}
			else
				return Utils.getStream(values.keySet()).filter(k -> k.is(role)).map(k -> k.asRole(role)).collect(SerCollectors.toMapWithValues(values::get));
		}

		@Override
		public DataPoint drop(Collection<? extends DataSetComponent<? extends NonIdentifier, ?, ?>> components)
		{
			Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> newVals = new HashMap<>(this);
			for (DataSetComponent<?, ?, ?> component : components)
				if (!component.is(Identifier.class))
					newVals.remove(component);

			return new DataPointImpl(getLineage(), new DataSetStructureBuilder(newVals.keySet()).build(), newVals);
		}

		@Override
		public DataPoint combine(DataPoint other, SerBiFunction<DataPoint, DataPoint, Lineage> lineageCombiner)
		{
			Objects.requireNonNull(other);

			Set<VTLAlias> thisComponentNames = keySet().stream().map(DataSetComponent::getAlias).collect(toSet());
			Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> finalMap = other.keySet().stream()
					.filter(c -> !thisComponentNames.contains(c.getAlias()))
					.collect(toConcurrentMap(c -> c, other::get, (a, b) -> null, () -> new ConcurrentHashMap<>(this)));
			DataSetStructure newStructure = new DataSetStructureBuilder(finalMap.keySet()).build();

			return new DataPointImpl(lineageCombiner.apply(this, other), newStructure, finalMap);
		}

		@Override
		public DataPoint keep(Collection<? extends DataSetComponent<? extends NonIdentifier, ?, ?>> components) throws VTLMissingComponentsException
		{
			Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> oper = new HashMap<>(getValues(Identifier.class));

			for (DataSetComponent<?, ?, ?> component : components)
				if (containsKey(component))
					oper.put(component, get(component));
				else
					throw new VTLMissingComponentsException(keySet(), component);

			return new DataPointImpl(getLineage() ,new DataSetStructureBuilder(oper.keySet()).build(), oper);
		}

		@Override
		public DataPoint rename(DataSetComponent<?, ?, ?> oldComponent, DataSetComponent<?, ?, ?> newComponent)
		{
			if (!containsKey(oldComponent))
				throw new VTLMissingComponentsException(keySet(), oldComponent);

			if (newComponent == null)
				throw new VTLException("rename: new omponent cannot be null");

			Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> newValues = new HashMap<>(this);
			ScalarValue<?, ?, ?, ?> value = newValues.remove(oldComponent);
			newValues.put(newComponent, value);
			return new DataPointImpl(getLineage(), new DataSetStructureBuilder(newValues.keySet()).build(), newValues);
		}

		@Override
		public boolean containsKey(Object key)
		{
			return values.containsKey(key);
		}

		@Override
		public boolean containsValue(Object value)
		{
			return values.containsValue(value);
		}

		@Override
		public Set<Map.Entry<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> entrySet()
		{
			return values.entrySet();
		}

		@Override
		public boolean equals(Object o)
		{
			return values.equals(o);
		}

		@Override
		public ScalarValue<?, ?, ?, ?> get(Object key)
		{
			if (values.containsKey(key))
				return values.get(key);
			else
				throw new VTLMissingComponentsException(values.keySet(), (DataSetComponent<?, ?, ?>) key);
		}

		@Override
		public int hashCode()
		{
			return hashCode;
		}

		@Override
		public int size()
		{
			return values.size();
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
