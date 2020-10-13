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

import static java.util.Collections.unmodifiableCollection;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.util.Triple;
import it.bancaditalia.oss.vtl.util.Utils;

public class DataStructureBuilder
{
	Set<DataStructureComponent<?, ?, ?>> components;

	public DataStructureBuilder()
	{
		components = ConcurrentHashMap.newKeySet();
	}

	public DataStructureBuilder(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		this();
		this.components.addAll(components);
	}

	public DataStructureBuilder(DataStructureComponent<?, ?, ?>... components)
	{
		this(Arrays.asList(components));
	}

	public DataStructureBuilder merge(DataStructureBuilder other)
	{
		return addComponents(other.components);
	}

	public DataStructureBuilder addComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		this.components.addAll(components);
		return this;
	}

	public <R extends Component, S extends ValueDomainSubset<D>, D extends ValueDomain> DataStructureBuilder addComponent(String name, Class<R> type, S domain)
	{
		this.components.add(new DataStructureComponentImpl<>(name, type, domain));
		return this;
	}

	public <R extends Component, S extends ValueDomainSubset<D>, D extends ValueDomain> DataStructureBuilder addComponent(Triple<String, Class<? extends R>, S> characteristics)
	{
		this.components.add(new DataStructureComponentImpl<>(characteristics.first(), characteristics.second(), characteristics.third()));
		return this;
	}

	public DataStructureBuilder addComponent(DataStructureComponent<?, ?, ?> component)
	{
		components.add(component);
		return this;
	}

	public DataStructureBuilder addComponents(DataStructureComponent<?, ?, ?>... components)
	{
		this.components.addAll(Arrays.asList(components));
		return this;
	}

	public DataStructureBuilder removeComponent(DataStructureComponent<?, ?, ?> component)
	{
		this.components.remove(component);
		return this;
	}

	public DataStructureBuilder removeComponents(DataStructureComponent<?, ?, ?>... components)
	{
		this.components.removeAll(Arrays.asList(components));
		return this;
	}

	public DataStructureBuilder removeComponents(Set<String> componentNames)
	{
		this.components.stream()
			.filter(c -> componentNames.contains(c.getName()))
			.forEach(this.components::remove);
		return this;
	}

	public DataStructureBuilder removeComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		this.components.removeAll(components);
		return this;
	}

	public DataSetMetadata build()
	{
		return new DataStructureImpl(components);
	}

	private static class DataStructureImpl extends AbstractSet<DataStructureComponent<?, ?, ?>> implements DataSetMetadata, Serializable
	{
		private static final long serialVersionUID = 1L;

		private final Map<String, DataStructureComponent<?, ?, ?>> components;

		private DataStructureImpl(Set<DataStructureComponent<?, ?, ?>> components)
		{
			this.components = Collections.unmodifiableMap(Utils.getStream(components)
				.map(Utils.toEntry(DataStructureComponent::getName, c -> c))
				.collect(Utils.entriesToMap(ConcurrentSkipListMap::new)));
		}
		
		@Override
		public <R extends Component> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
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
		public DataSetMetadata swapComponent(DataStructureComponent<?, ?, ?> oldComponent, DataStructureComponent<?, ?, ?> newComponent)
		{
			return new DataStructureBuilder(this).removeComponent(oldComponent).addComponents(newComponent).build();
		}

		@Override
		public DataSetMetadata keep(String... names)
		{
			Map<Boolean, List<DataStructureComponent<?, ?, ?>>> toKeep = Utils.getStream(names)
					.map(components::get)
					.filter(Objects::nonNull)
					.collect(Collectors.partitioningBy(c -> c.is(Identifier.class)));
			
			return new DataStructureBuilder(getComponents(Identifier.class))
					.addComponents(toKeep.get(false))
					.addComponents(toKeep.get(true).stream()
							.map(c -> new DataStructureComponentImpl<>(c.getDomain().getVarName(), Measure.class, c.getDomain()))
							.collect(Collectors.toList()))
					.build();
		}

		@Override
		public DataSetMetadata drop(Collection<String> names)
		{
			final Set<? extends DataStructureComponent<?, ?, ?>> filter = Utils.getStream(names)
					.map(components::get)
					.filter(Objects::nonNull)
					.filter(c -> !c.is(Identifier.class))
					.collect(toSet());

			return new DataStructureBuilder(this).removeComponents(filter).build();
		}

		@Override
		public DataSetMetadata membership(String name)
		{
			DataStructureComponent<?, ?, ?> component = components.get(name);

			if (component == null)
				throw new VTLMissingComponentsException(name, components.values());

			if (component.is(Measure.class))
				return new DataStructureBuilder().addComponents(component).addComponents(getComponents(Identifier.class)).build();
			else
				return new DataStructureBuilder().addComponents(new DataStructureComponentImpl<>(component.getDomain().getVarName(), Measure.class, component.getDomain()))
						.addComponents(getComponents(Identifier.class)).build();
		}

		@Override
		public DataSetMetadata joinForOperators(DataSetMetadata VTLDataSetMetadata)
		{
			return new DataStructureBuilder(this).addComponents(VTLDataSetMetadata).build();
		}

		@Override
		public DataSetMetadata subspace(Collection<? extends DataStructureComponent<Identifier, ?, ?>> subspace)
		{
			return new DataStructureBuilder().addComponents(components.values().parallelStream().filter(c -> !subspace.contains(c)).collect(toSet())).build();
		}

		@Override
		public DataSetMetadata rename(DataStructureComponent<?, ?, ?> component, String newName)
		{
			return new DataStructureBuilder(this).removeComponent(component).addComponent(component.rename(newName)).build();
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
			return unmodifiableCollection(components.values()).iterator();
		}

		@Override
		public int size()
		{
			return components.size();
		}

		@Override
		public <S extends ValueDomainSubset<D>, D extends ValueDomain> DataSetMetadata pivot(DataStructureComponent<Identifier, StringEnumeratedDomainSubset, StringDomain> identifier,
				DataStructureComponent<Measure, S, D> measure)
		{
			return Utils.getStream(identifier.getDomain().getCodeItems())
					.map(i -> new DataStructureComponentImpl<>(i.get(), Measure.class, measure.getDomain()))
					.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge)
					.addComponents(getComponents(Identifier.class))
					.removeComponent(identifier)
					.removeComponent(measure)
					.build();
		}
	}

	public static Collector<DataStructureComponent<?, ?, ?>, ?, DataSetMetadata> toDataStructure(DataStructureComponent<?, ?, ?>... additionalComponents)
	{
		return Collector.of(DataStructureBuilder::new, DataStructureBuilder::addComponent, 
				DataStructureBuilder::merge, dsb -> dsb.addComponents(additionalComponents).build(), 
				UNORDERED, CONCURRENT);
	}

	public static Collector<DataStructureComponent<?, ?, ?>, ?, DataSetMetadata> toDataStructure(Collection<? extends DataStructureComponent<?, ?, ?>> additionalComponents)
	{
		return Collector.of(DataStructureBuilder::new, DataStructureBuilder::addComponent, 
				DataStructureBuilder::merge, dsb -> dsb.addComponents(additionalComponents).build(), 
				UNORDERED, CONCURRENT);
	}
}
