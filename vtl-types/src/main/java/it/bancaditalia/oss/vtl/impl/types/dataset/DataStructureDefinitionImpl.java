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

import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.DataStructureDefinition;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class DataStructureDefinitionImpl extends AbstractSet<DataStructureComponent<?>> implements DataStructureDefinition
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias alias;
	private final Map<VTLAlias, DataStructureComponent<?>> byAlias = new HashMap<>();
	private final Map<Class<? extends Component>, Set<DataStructureComponent<?>>> byRole = new HashMap<>();
	
	public DataStructureDefinitionImpl(VTLAlias alias, Collection<? extends DataStructureComponent<?>> components)
	{
		this.alias = alias;
		
		byRole.put(Identifier.class, new HashSet<>());
		byRole.put(Measure.class, new HashSet<>());
		byRole.put(Attribute.class, new HashSet<>());
		byRole.put(ViralAttribute.class, new HashSet<>());

		for (DataStructureComponent<?> comp: components)
		{
			byAlias.put(comp.getAlias(), comp);
			for (Class<? extends Component> role: List.of(Identifier.class, Measure.class, Attribute.class, ViralAttribute.class))
				if (role.isAssignableFrom(comp.getRole()))
					byRole.get(role).add(comp);
		}
	}

	@Override
	public VTLAlias getAlias()
	{
		return alias;
	}
	
	@Override
	public int size()
	{
		return byAlias.size();
	}

	@Override
	public Iterator<DataStructureComponent<?>> iterator()
	{
		return unmodifiableCollection(byAlias.values()).iterator();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Component> Set<DataStructureComponent<R>> getComponents(Class<R> role)
	{
		return (Set<DataStructureComponent<R>>) (Set<? extends DataStructureComponent<?>>) unmodifiableSet(byRole.get(role));
	}

	@Override
	public boolean contains(VTLAlias alias)
	{
		return byAlias.containsKey(alias);
	}

	@Override
	public Optional<DataStructureComponent<?>> getComponent(VTLAlias alias)
	{
		return Optional.ofNullable(byAlias.get(alias));
	} 
}
