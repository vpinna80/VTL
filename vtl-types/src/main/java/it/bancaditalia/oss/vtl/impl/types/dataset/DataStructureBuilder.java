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

import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class DataStructureBuilder
{
	private Set<DataStructureComponent<?, ?, ?>> components;

	public static SerCollector<DataStructureComponent<?, ?, ?>, ?, DataSetMetadata> toDataStructure(DataStructureComponent<?, ?, ?>... additionalComponents)
	{
		return SerCollector.of(DataStructureBuilder::new, DataStructureBuilder::addComponent, 
				DataStructureBuilder::merge, dsb -> dsb.addComponents(additionalComponents).build(), 
				EnumSet.of(UNORDERED, CONCURRENT));
	}

	public static SerCollector<DataStructureComponent<?, ?, ?>, ?, DataSetMetadata> toDataStructure(Collection<? extends DataStructureComponent<?, ?, ?>> additionalComponents)
	{
		return SerCollector.of(DataStructureBuilder::new, DataStructureBuilder::addComponent, 
				DataStructureBuilder::merge, dsb -> dsb.addComponents(additionalComponents).build(), 
				EnumSet.of(UNORDERED, CONCURRENT));
	}

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

	public DataStructureBuilder addComponent(DataStructureComponent<?, ?, ?> component)
	{
		components.add(component);
		return this;
	}

	public DataStructureBuilder addComponents(CommonComponents... components)
	{
		return addComponents(Arrays.stream(components).map(CommonComponents::get).collect(toList()));
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

	public DataStructureBuilder removeComponents(Set<VTLAlias> componentNames)
	{
		this.components.stream()
			.filter(c -> componentNames.contains(c.getVariable().getAlias()))
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

		private final Map<VTLAlias, DataStructureComponent<?, ?, ?>> byName;
		private final Map<Class<? extends Component>, Set<DataStructureComponent<?, ?, ?>>> byRole;
		
		private transient Set<Set<?>> cache;
		
		private DataStructureImpl(Set<DataStructureComponent<?, ?, ?>> components)
		{
			byName = components.stream()
				.sorted(DataStructureComponent::byNameAndRole)
				.collect(toMap(c -> c.getVariable().getAlias(), identity(), LinkedHashMap::new));
			byRole = components.stream()
					.collect(groupingBy(DataStructureComponent::getRole, DataStructureBuilder::createEmptyStructure, toSet()));
			byRole.get(Attribute.class).addAll(byRole.get(ViralAttribute.class));
			
			int totalSize = byRole.values().stream().mapToInt(Collection::size).sum() - byRole.get(ViralAttribute.class).size();
			if (totalSize != components.size())
				throw new IllegalStateException(totalSize  + " != " + components.size());
		}
		
		@Override
		public <R extends Component> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
		{
			Set<? extends DataStructureComponent<?, ?, ?>> result;
			
			if (Component.class == typeOfComponent)
				result = this;
			else if (Identifier.class == typeOfComponent)
				result = byRole.get(Identifier.class);
			else if (Measure.class == typeOfComponent)
				result = byRole.get(Measure.class);
			else if (Attribute.class == typeOfComponent)
				result = byRole.get(Attribute.class);
			else if (ViralAttribute.class == typeOfComponent)
				result = byRole.get(ViralAttribute.class);
			else
				result = byName.values().stream()
						.filter(c -> typeOfComponent.isAssignableFrom(c.getRole()))
						.collect(toSet());
			
			@SuppressWarnings("unchecked")
			Set<DataStructureComponent<R, ?, ?>> unchk = (Set<DataStructureComponent<R, ?, ?>>) result;
			
			return unchk;
		}

		@Override
		public Optional<DataStructureComponent<?, ?, ?>> getComponent(VTLAlias alias)
		{
			return Optional.ofNullable(byName.get(alias));
		}

		@Override
		public DataSetMetadata membership(VTLAlias alias)
		{
			DataStructureComponent<?, ?, ?> component = byName.get(alias);

			if (component == null)
				throw new VTLMissingComponentsException(alias, byName.values());

			DataStructureBuilder commonComponents = new DataStructureBuilder()
					.addComponents(getIDs())
					.addComponents(getComponents(ViralAttribute.class));
			
			if (component.is(Measure.class))
				commonComponents = commonComponents.addComponent(component);
			else
				commonComponents = commonComponents.addComponent(component.getVariable().getDomain().getDefaultVariable().as(Measure.class));
			
			return commonComponents.build();
		}

		@Override
		public DataSetMetadata joinForOperators(DataSetMetadata VTLDataSetMetadata)
		{
			return new DataStructureBuilder(this).addComponents(VTLDataSetMetadata).build();
		}

		@Override
		public DataSetMetadata subspace(Collection<? extends DataStructureComponent<Identifier, ?, ?>> subspace)
		{
			return new DataStructureBuilder().addComponents(byName.values().parallelStream().filter(c -> !subspace.contains(c)).collect(toSet())).build();
		}

		@Override
		public String toString()
		{
			return byName.values().toString();
		}

		@Override
		public int hashCode()
		{
			return super.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (cache == null)
				cache = newSetFromMap(new WeakHashMap<>());
			return cache.contains(obj) || super.equals(obj) && cache.add((Set<?>) obj);
		}

		@Override
		public boolean contains(VTLAlias alias)
		{
			return byName.containsKey(alias);
		}

		@Override
		public Iterator<DataStructureComponent<?, ?, ?>> iterator()
		{
			return byName.values().iterator();
		}
		
		@Override
		public int size()
		{
			return byName.size();
		}
	}

	private static Map<Class<? extends Component>, Set<DataStructureComponent<?, ?, ?>>> createEmptyStructure()
	{
		Map<Class<? extends Component>, Set<DataStructureComponent<?, ?, ?>>> empty = new HashMap<>();
		empty.put(Identifier.class, new HashSet<>());
		empty.put(Measure.class, new HashSet<>());
		empty.put(Attribute.class, new HashSet<>());
		empty.put(ViralAttribute.class, new HashSet<>());
		return empty;
	}
}
