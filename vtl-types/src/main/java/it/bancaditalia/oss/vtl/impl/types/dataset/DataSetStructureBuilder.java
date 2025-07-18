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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.getDefaultMeasure;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class DataSetStructureBuilder
{
	private Set<DataSetComponent<?, ?, ?>> components;

	public static SerCollector<DataSetComponent<?, ?, ?>, ?, DataSetStructure> toDataStructure(DataSetComponent<?, ?, ?>... additionalComponents)
	{
		return SerCollector.of(DataSetStructureBuilder::new, DataSetStructureBuilder::addComponent, 
				DataSetStructureBuilder::merge, dsb -> dsb.addComponents(additionalComponents).build(), 
				EnumSet.of(UNORDERED, CONCURRENT));
	}

	public static SerCollector<DataSetComponent<?, ?, ?>, ?, DataSetStructure> toDataStructure(Collection<? extends DataSetComponent<?, ?, ?>> additionalComponents)
	{
		return SerCollector.of(DataSetStructureBuilder::new, DataSetStructureBuilder::addComponent, 
				DataSetStructureBuilder::merge, dsb -> dsb.addComponents(additionalComponents).build(), 
				EnumSet.of(UNORDERED, CONCURRENT));
	}

	public DataSetStructureBuilder()
	{
		components = ConcurrentHashMap.newKeySet();
	}

	public DataSetStructureBuilder(Collection<? extends DataSetComponent<?, ?, ?>> components)
	{
		this();
		this.components.addAll(components);
	}

	public DataSetStructureBuilder(DataSetComponent<?, ?, ?>... components)
	{
		this(Arrays.asList(components));
	}

	public DataSetStructureBuilder merge(DataSetStructureBuilder other)
	{
		return addComponents(other.components);
	}

	public DataSetStructureBuilder addComponents(Collection<? extends DataSetComponent<?, ?, ?>> components)
	{
		this.components.addAll(components);
		return this;
	}

	public DataSetStructureBuilder addComponent(DataSetComponent<?, ?, ?> component)
	{
		components.add(component);
		return this;
	}

	public DataSetStructureBuilder addComponents(CommonComponents... components)
	{
		return addComponents(Arrays.stream(components).map(CommonComponents::get).collect(toList()));
	}

	public DataSetStructureBuilder addComponents(DataSetComponent<?, ?, ?>... components)
	{
		this.components.addAll(Arrays.asList(components));
		return this;
	}

	public DataSetStructureBuilder removeComponent(DataSetComponent<?, ?, ?> component)
	{
		this.components.remove(component);
		return this;
	}

	public DataSetStructureBuilder removeComponents(DataSetComponent<?, ?, ?>... components)
	{
		this.components.removeAll(Arrays.asList(components));
		return this;
	}

	public DataSetStructureBuilder removeComponents(Set<VTLAlias> componentNames)
	{
		this.components.stream()
			.filter(c -> componentNames.contains(c.getAlias()))
			.forEach(this.components::remove);
		return this;
	}

	public DataSetStructureBuilder removeComponents(Collection<? extends DataSetComponent<?, ?, ?>> components)
	{
		this.components.removeAll(components);
		return this;
	}

	public DataSetStructure build()
	{
		return new DataSetMetadataImpl(components);
	}

	private static class DataSetMetadataImpl extends AbstractSet<DataSetComponent<?, ?, ?>> implements DataSetStructure, Serializable
	{
		private static final long serialVersionUID = 1L;

		private final Map<VTLAlias, DataSetComponent<?, ?, ?>> byName;
		private final Map<Class<? extends Component>, Set<DataSetComponent<?, ?, ?>>> byRole;
		
		private transient Set<Set<?>> cache;
		
		private DataSetMetadataImpl(Set<DataSetComponent<?, ?, ?>> components)
		{
			byName = components.stream()
				.sorted(DataSetComponent::byNameAndRole)
				.collect(collectingAndThen(toMap(c -> c.getAlias(), identity(), LinkedHashMap::new), Collections::unmodifiableMap));
			byRole = components.stream()
					.collect(groupingBy(DataSetComponent::getRole, DataSetStructureBuilder::createEmptyStructure, toSet()));
			byRole.get(Attribute.class).addAll(byRole.get(ViralAttribute.class));
			byRole.replaceAll((k, v) -> unmodifiableSet(v));
			
			int totalSize = byRole.values().stream().mapToInt(Collection::size).sum() - byRole.get(ViralAttribute.class).size();
			if (totalSize != components.size())
				throw new IllegalStateException(totalSize  + " != " + components.size());
		}
		
		@Override
		public <R extends Component> Set<DataSetComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
		{
			Set<? extends DataSetComponent<?, ?, ?>> result;
			
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
			Set<DataSetComponent<R, ?, ?>> unchk = (Set<DataSetComponent<R, ?, ?>>) result;
			
			return unchk;
		}

		@Override
		public Optional<DataSetComponent<?, ?, ?>> getComponent(VTLAlias alias)
		{
			return Optional.ofNullable(byName.get(alias));
		}

		@Override
		public DataSetStructure membership(VTLAlias alias)
		{
			DataSetComponent<?, ?, ?> component = byName.get(alias);

			if (component == null)
				throw new VTLMissingComponentsException(byName.values(), alias);

			DataSetStructureBuilder commonComponents = new DataSetStructureBuilder()
					.addComponents(getIDs())
					.addComponents(getComponents(ViralAttribute.class));
			
			if (component.is(Measure.class))
				commonComponents = commonComponents.addComponent(component);
			else {
				ValueDomainSubset<?, ?> domain = component.getDomain();
				commonComponents = commonComponents.addComponent(getDefaultMeasure(domain));
			}
			
			return commonComponents.build();
		}

		@Override
		public DataSetStructure joinForOperators(DataSetStructure VTLDataSetMetadata)
		{
			return new DataSetStructureBuilder(this).addComponents(VTLDataSetMetadata).build();
		}

		@Override
		public DataSetStructure subspace(Collection<? extends DataSetComponent<Identifier, ?, ?>> subspace)
		{
			return new DataSetStructureBuilder().addComponents(byName.values().parallelStream().filter(c -> !subspace.contains(c)).collect(toSet())).build();
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
		public Iterator<DataSetComponent<?, ?, ?>> iterator()
		{
			return byName.values().iterator();
		}
		
		@Override
		public int size()
		{
			return byName.size();
		}
	}

	private static Map<Class<? extends Component>, Set<DataSetComponent<?, ?, ?>>> createEmptyStructure()
	{
		Map<Class<? extends Component>, Set<DataSetComponent<?, ?, ?>>> empty = new HashMap<>();
		empty.put(Identifier.class, new HashSet<>());
		empty.put(Measure.class, new HashSet<>());
		empty.put(Attribute.class, new HashSet<>());
		empty.put(ViralAttribute.class, new HashSet<>());
		return empty;
	}
}
