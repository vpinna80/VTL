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
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.partitioningBy;
//import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.Utils;

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

		private final Map<String, DataStructureComponent<?, ?, ?>> byName;
		private final Map<Class<? extends ComponentRole>, Set<DataStructureComponent<?, ?, ?>>> byRole;

		private DataStructureImpl(Set<DataStructureComponent<?, ?, ?>> components)
		{
			this.byName = components.stream()
				.sorted(DataStructureComponent::byNameAndRole)
				.collect(toMap(DataStructureComponent::getName, identity(), LinkedHashMap::new));
			this.byRole = components.stream()
					.collect(groupingBy(DataStructureComponent::getRole, DataStructureBuilder::createEmptyStructure, toSet()));
			this.byRole.get(Attribute.class).addAll(byRole.get(ViralAttribute.class));
		}
		
		@Override
		public <R extends ComponentRole> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
		{
			Set<? extends DataStructureComponent<?, ?, ?>> result;
			
			if (ComponentRole.class == typeOfComponent)
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
		public Optional<DataStructureComponent<?, ?, ?>> getComponent(String alias)
		{
			return Optional.ofNullable(byName.get(alias));
		}

		@Override
		public DataSetMetadata keep(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> comps)
		{
			Map<Boolean, List<DataStructureComponent<?, ?, ?>>> toKeep = Utils.getStream(comps)
					.collect(partitioningBy(c -> c.is(Identifier.class)));
			
			return new DataStructureBuilder(getIDs())
					.addComponents(toKeep.get(false))
					.addComponents(toKeep.get(true).stream()
							.map(DataStructureComponent::createMeasureFrom)
							.collect(toList()))
					.build();
		}

		@Override
		public DataSetMetadata drop(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> comps)
		{
			return new DataStructureBuilder(this).removeComponents(comps).build();
		}

		@Override
		public DataSetMetadata membership(String alias)
		{
			DataStructureComponent<?, ?, ?> component = byName.get(alias);

			if (component == null)
				throw new VTLMissingComponentsException(alias, byName.values());

			if (component.is(Measure.class))
				return new DataStructureBuilder().addComponents(component).addComponents(getIDs()).build();
			else
				return new DataStructureBuilder().addComponents(component.createMeasureFrom())
						.addComponents(getIDs()).build();
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
		public DataSetMetadata rename(DataStructureComponent<?, ?, ?> component, String newName)
		{
			return new DataStructureBuilder(this).removeComponent(component).addComponent(component.rename(newName)).build();
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
			return super.equals(obj);
		}

		@Override
		public boolean contains(String alias)
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

		@Override
		public <S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataSetMetadata pivot(DataStructureComponent<Identifier, ? extends StringEnumeratedDomainSubset<?, ?, ?>, StringDomain> identifier,
				DataStructureComponent<Measure, S, D> measure)
		{
			return Utils.getStream(identifier.getDomain().getCodeItems())
					.map(item -> new DataStructureComponentImpl<>(item.get().toString(), Measure.class, measure.getDomain()))
					.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge)
					.addComponents(getIDs())
					.removeComponent(identifier)
					.removeComponent(measure)
					.build();
		}
	}

	private static Map<Class<? extends ComponentRole>, Set<DataStructureComponent<?, ?, ?>>> createEmptyStructure()
	{
		Map<Class<? extends ComponentRole>, Set<DataStructureComponent<?, ?, ?>>> empty = new HashMap<>();
		empty.put(Identifier.class, new HashSet<>());
		empty.put(Measure.class, new HashSet<>());
		empty.put(Attribute.class, new HashSet<>());
		empty.put(ViralAttribute.class, new HashSet<>());
		return empty;
	}
}
