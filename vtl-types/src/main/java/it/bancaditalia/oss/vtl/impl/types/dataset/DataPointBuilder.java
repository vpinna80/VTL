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

import static it.bancaditalia.oss.vtl.util.Utils.toMapWithValues;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.Utils;

public class DataPointBuilder 
{
	private final ConcurrentHashMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> delegate;

	private boolean built = false;

	public DataPointBuilder()
	{
		 delegate = new ConcurrentHashMap<>();
	}
	
	public DataPointBuilder(Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?>> keys)
	{
		 delegate = new ConcurrentHashMap<>(keys);
	}
	
	private synchronized DataPointBuilder checkState()
	{
		if (built)
			throw new IllegalStateException("DataPoint already built.");
		return this;
	}

	public DataPointBuilder merge(DataPointBuilder other)
	{
		other.delegate.forEach(delegate::putIfAbsent);
		return checkState();
	}

	public DataPointBuilder addAll(Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?>> values)
	{
		values.forEach(delegate::putIfAbsent);
		return checkState();
	}

	public DataPointBuilder add(Entry<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?>> value)
	{
		delegate.putIfAbsent(value.getKey(), value.getValue());
		return checkState();
	}
	
	public DataPointBuilder add(DataStructureComponent<?, ?, ?> component, ScalarValue<?, ?, ?> value)
	{
		delegate.putIfAbsent(component, value);
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

	public synchronized DataPoint build(Collection<? extends DataStructureComponent<?, ?, ?>> structure)
	{
		if (built)
			throw new IllegalStateException("DataPoint already built");
		built  = true;
		return new DataPointImpl(structure, new HashMap<>(delegate));
	}
	
	@Override
	public String toString()
	{
		return DataPointBuilder.class.getSimpleName() + delegate.toString();
	}

	private static class DataPointImpl extends AbstractMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> implements DataPoint, Serializable
	{
		private static final long serialVersionUID = 1L;
		private static final Logger LOGGER = LoggerFactory.getLogger(DataPointImpl.class);
		
		private final Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> dpValues;

		
		private DataPointImpl(Collection<? extends DataStructureComponent<?, ?, ?>> structure, Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> values)
		{
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> filledValues = new ConcurrentHashMap<>(values);
			Utils.getStream(structure)
				.filter(c -> !c.is(Identifier.class))
				.filter(c -> !values.containsKey(c))
				.forEach(c -> filledValues.put(c, NullValue.instance(c.getDomain())));
			
			this.dpValues = filledValues;
			
			Utils.getStream(this.dpValues.keySet())
				.filter(c -> !structure.contains(c))
				.map(c -> new SimpleEntry<>(c, new IllegalStateException("Component " + c + " has a value but is not defined on " + structure)))
				.peek(e -> LOGGER.error("Component {} has a value but is not defined on {} in datapoint {}", e.getKey(), structure, values, e.getValue()))
				.map(Map.Entry::getValue)
				.findAny()
				.ifPresent(e -> { 
					throw e; 
				});
			
			Set<DataStructureComponent<?, ?, ?>> missing = new HashSet<>(structure);
			missing.removeAll(this.dpValues.keySet());
			if (missing.size() > 0)
				throw new VTLMissingComponentsException(missing, this.dpValues);
		}

		@Override
		public <R extends ComponentRole> Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?>> getValues(Class<R> role)
		{
			return Utils.getStream(dpValues.keySet())
					.filter(k -> k.is(role))
					.map(k -> k.as(role))
					.collect(toMapWithValues(dpValues::get));
		}
		
		@Override
		public DataPoint dropComponents(Set<DataStructureComponent<?, ?, ?>> components)
		{
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> newVals = new HashMap<>(this);
			for (DataStructureComponent<?, ?, ?> component: components)
				if (!component.is(Identifier.class))
					newVals.remove(component);
			
			return new DataPointImpl(newVals.keySet(), newVals);
		}

		@Override
		public DataPoint merge(DataPoint other)
		{
			Objects.requireNonNull(other);
			
			Set<String> thisNames = keySet().stream()
					.map(DataStructureComponent::getName)
					.collect(Collectors.toSet());
			
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> finalMap = other.keySet().stream()
					.filter(c -> !thisNames.contains(c.getName()))
					.collect(toConcurrentMap(c -> c, other::get, (a, b) -> null, () -> new ConcurrentHashMap<>(this)));
			
			return new DataPointImpl(finalMap.keySet(), finalMap);
		}

		@Override
		public DataPoint keep(Collection<? extends DataStructureComponent<?, ?, ?>> components) throws VTLMissingComponentsException
		{
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> oper = new HashMap<>(getValues(Identifier.class));
			
			for (DataStructureComponent<?, ?, ?> component: components)
				if (containsKey(component))
					oper.put(component, get(component));
				else
					throw new VTLMissingComponentsException(component, keySet()); 
			
			return new DataPointImpl(oper.keySet(), oper);
		}

		@Override
		public DataPoint renameComponent(DataStructureComponent<?, ?, ?> oldComponent, DataStructureComponent<?, ?, ?> newComponent)
		{
			if (!containsKey(oldComponent))
				throw new VTLMissingComponentsException(oldComponent, keySet());
			
			if (newComponent == null)
				throw new VTLException("rename: new omponent cannot be null");
			
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> newValues = new HashMap<>(this);
			ScalarValue<?, ?, ?> value = newValues.remove(oldComponent);
			newValues.put(newComponent, value);
			return new DataPointImpl(newValues.keySet(), newValues);
		}
		
		@Override
		public DataPoint alter(Map<? extends DataStructureComponent<Measure, ?, ?>, ? extends ScalarValue<?, ?, ?>> measures)
		{
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> result = new HashMap<>(getValues(Identifier.class));
			result.putAll(measures);
			
			return new DataPointImpl(result.keySet(), result); 
		}
		
		@Override
		public DataPoint subspace(Set<? extends DataStructureComponent<Identifier, ?, ?>> subspace)
		{
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> result = new HashMap<>(this);
			result.keySet().removeAll(subspace);
			
			return new DataPointImpl(result.keySet(), result);
		}
		
		@Override
		public DataPoint replaceComponent(DataStructureComponent<?, ?, ?> component, ScalarValue<?, ?, ?> value)
		{
			if (!containsKey(component))
				throw new VTLMissingComponentsException(component, keySet());
			
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> newValues = new HashMap<>(this);
			newValues.put(component, value);
			return new DataPointImpl(keySet(), newValues);
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
		public Set<Map.Entry<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>>> entrySet()
		{
			return dpValues.entrySet();
		}

		@Override
		public boolean equals(Object o)
		{
			return dpValues.equals(o);
		}

		@Override
		public ScalarValue<?, ?, ?> get(Object key)
		{
			return dpValues.get(key);
		}

		@Override
		public int hashCode()
		{
			return dpValues.hashCode();
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
					.peek(e -> Objects.nonNull(e.getKey()))
					.peek(e -> Objects.nonNull(e.getValue()))
					.map(Entry::toString)
					.collect(joining(", ", "{ ", " }"));
		}
	}
}
