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

import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringCodeList;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.util.Triple;
import it.bancaditalia.oss.vtl.util.Utils;

public class DataStructureImpl extends AbstractSet<DataStructureComponent<?, ?, ?>> implements VTLDataSetMetadata, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(DataStructureImpl.class);

	public static class Builder
	{
		KeySetView<DataStructureComponent<?, ?, ?>, Boolean> components;

		public Builder()
		{
			components = ConcurrentHashMap.newKeySet();
		}

		public Builder(Collection<? extends DataStructureComponent<?, ?, ?>> components)
		{
			this();
			this.components.addAll(components);
		}

		public Builder merge(Builder other)
		{
			return addComponents(other.components);
		}

		public Builder addComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
		{
			this.components.addAll(components);
			return this;
		}

		public <R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> Builder addComponent(String name, Class<R> type, S domain)
		{
			this.components.add(new DataStructureComponentImpl<>(name, type, domain));
			return this;
		}

		public <R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> Builder addComponent(Triple<String, Class<? extends R>, S> characteristics)
		{
			this.components.add(new DataStructureComponentImpl<>(characteristics.first(), characteristics.second(), characteristics.third()));
			return this;
		}

		public Builder addComponent(DataStructureComponent<?, ?, ?> component)
		{
			components.add(component);
			return this;
		}

		public Builder addComponents(DataStructureComponent<?, ?, ?>... components)
		{
			this.components.addAll(Arrays.asList(components));
			return this;
		}

		public Builder removeComponent(DataStructureComponent<?, ?, ?> component)
		{
			this.components.remove(component);
			return this;
		}
		public Builder removeComponents(DataStructureComponent<?, ?, ?>... components)
		
		{
			this.components.removeAll(Arrays.asList(components));
			return this;
		}

		public Builder removeComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
		{
			this.components.removeAll(components);
			return this;
		}

		public VTLDataSetMetadata build()
		{
			return new DataStructureImpl(components);
		}
	}

	private final Map<String, DataStructureComponent<?, ?, ?>>                 components        = Collections.synchronizedMap(new TreeMap<>());
	private final Set<Set<? extends DataStructureComponent<Identifier, ?, ?>>> registeredIndexes = new HashSet<>();

	private static final AtomicLong counter = new AtomicLong();
	public final long              count   = counter.incrementAndGet();

	private DataStructureImpl(Set<? extends DataStructureComponent<?, ?, ?>> components)
	{
		Utils.getStream(components).forEach(c -> this.components.put(c.getName(), c));
	}
	
	@Override
	public <R extends ComponentRole> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
	{
		return Utils.getStream(components.values())
				.filter(c -> c.is(typeOfComponent))
				.map(c -> c.as(typeOfComponent))
				.collect(toSet());
	}

	@Override
	public Optional<DataStructureComponent<?, ?, ?>> getComponent(String component)
	{
		String stripped = component.replaceAll("'(.*)'", "$1");
		return Optional.ofNullable(components.get(stripped));
	}

	@Override
	public boolean containsComponent(String componentName)
	{
		return components.containsKey(componentName);
	}

	@Override
	public VTLDataSetMetadata swapComponent(DataStructureComponent<?, ?, ?> oldComponent, DataStructureComponent<?, ?, ?> newComponent)
	{
		return new Builder(this).removeComponent(oldComponent).addComponents(newComponent).build();
	}

	@Override
	public VTLDataSetMetadata keep(String... names)
	{
		Map<Boolean, List<DataStructureComponent<?, ?, ?>>> toKeep = Utils.getStream(names)
				.map(components::get)
				.filter(Objects::nonNull)
				.collect(Collectors.partitioningBy(c -> c.is(Identifier.class)));
		
		return new Builder(getComponents(Identifier.class))
				.addComponents(toKeep.get(false))
				.addComponents(toKeep.get(true).stream()
						.map(c -> new DataStructureComponentImpl<>(c.getDomain().getVarName(), Measure.class, c.getDomain()))
						.collect(Collectors.toList()))
				.build();
	}

	@Override
	public VTLDataSetMetadata drop(Collection<String> names)
	{
		final Set<? extends DataStructureComponent<?, ?, ?>> filter = Utils.getStream(names)
				.map(components::get)
				.filter(Objects::nonNull)
				.filter(c -> !c.is(Identifier.class))
				.collect(toSet());

		return new Builder(this).removeComponents(filter).build();
	}

	@Override
	public VTLDataSetMetadata membership(String name)
	{
		DataStructureComponent<?, ?, ?> component = components.get(name);

		if (component == null)
			throw new VTLMissingComponentsException(name, components.values());

		if (component.is(Measure.class))
			return new Builder().addComponents(component).addComponents(getComponents(Identifier.class)).build();
		else
			return new Builder().addComponents(new DataStructureComponentImpl<>(component.getDomain().getVarName(), Measure.class, component.getDomain()))
					.addComponents(getComponents(Identifier.class)).build();
	}

	@Override
	public VTLDataSetMetadata joinForOperators(DataStructure dataStructure)
	{
		return new Builder(this).addComponents(dataStructure).build();
	}

	@Override
	public VTLDataSetMetadata subspace(Collection<? extends DataStructureComponent<Identifier, ?, ?>> subspace)
	{
		return new Builder().addComponents(components.values().parallelStream().filter(c -> !subspace.contains(c)).collect(toSet())).build();
	}

	@Override
	public VTLDataSetMetadata rename(DataStructureComponent<?, ?, ?> component, String newName)
	{
		return new Builder(this).removeComponent(component).addComponent(component.rename(newName)).build();
	}

	@Override
	public String toString()
	{
		return components.values().toString();
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((components == null) ? 0 : components.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataStructureImpl other = (DataStructureImpl) obj;
		if (components == null)
		{
			if (other.components != null)
				return false;
		}
		else if (!components.equals(other.components))
			return false;
		return true;
	}

	@Override
	public boolean contains(String component)
	{
		return components.containsKey(component);
	}

	@Override
	public Iterator<DataStructureComponent<?, ?, ?>> iterator()
	{
		return Collections.unmodifiableCollection(components.values()).iterator();
	}

	@Override
	public int size()
	{
		return components.size();
	}

	@Override
	public <S extends ValueDomainSubset<D>, D extends ValueDomain> VTLDataSetMetadata pivot(DataStructureComponent<Identifier, StringCodeList, StringDomain> identifier,
			DataStructureComponent<Measure, S, D> measure)
	{
		return Utils.getStream(identifier.getDomain().getCodeItems()).map(i -> new DataStructureComponentImpl<>(i.get(), Measure.class, measure.getDomain()))
				.reduce(new Builder(), Builder::addComponent, Builder::merge).addComponents(getComponents(Identifier.class)).removeComponent(identifier).removeComponent(measure)
				.build();
	}

	@Override
	public void registerIndex(Set<? extends DataStructureComponent<Identifier, ?, ?>> keys)
	{
		if (!registeredIndexes.contains(keys))
			registeredIndexes.add(keys);
		LOGGER.debug("{}: {}", count, components.values());
	}

	@Override
	public Set<Set<? extends DataStructureComponent<Identifier, ?, ?>>> getRequestedIndexes()
	{
		return registeredIndexes;
	}
}
