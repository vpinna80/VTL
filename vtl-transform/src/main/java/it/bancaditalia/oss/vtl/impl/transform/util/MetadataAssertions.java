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
package it.bancaditalia.oss.vtl.impl.transform.util;

import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class MetadataAssertions<R extends ComponentRole, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Supplier<Set<? extends DataStructureComponent<R, S, D>>>
{
	private final Set<DataStructureComponent<R, S, D>> components;
	private final Class<R> role;
	private final S domain;
	private final String name;

	public static <R extends ComponentRole, S extends ValueDomainSubset<S, D>, D extends ValueDomain> MetadataAssertions<R, S, D> asserts(String name, Set<DataStructureComponent<R, S, D>> components)
	{
		return new MetadataAssertions<>(name, components, null, null);
	}
	
	private MetadataAssertions(String name, Set<? extends DataStructureComponent<R, S, D>> components, Class<R> role, S domain)
	{
		this.components = new HashSet<>(components);
		this.role = role;
		this.domain = domain;
		this.name = name;
	}
	
	public Set<? extends DataStructureComponent<R,S,D>> get()
	{
		return components;
	}

	public DataSetMetadata getAsStructure()
	{
		return new DataStructureBuilder(get()).build();
	}

	public MetadataAssertions<R, S, D> withAtLeastOne()
	{
		if (components.isEmpty())
			throw new VTLExpectedComponentException(role, components);
		
		return this;
	}

	public <R1 extends R> MetadataAssertions<R1, S, D> withRole(Class<R1> role)
	{
		return new MetadataAssertions<>(name, this.components.stream()
				.filter(c -> c.is(role))
				.map(c -> c.asRole(role))
				.collect(toSet()), role, domain);
	}

	public <R1 extends R> MetadataAssertions<R1, S, D> withRoleCheck(Class<R1> role)
	{
		MetadataAssertions<R1, S, D> filtered = withRole(role);

		if (filtered.get().size() != this.components.size())
		{
			this.components.removeAll(filtered.get());
			throw new VTLIncompatibleRolesException(name, this.components.iterator().next(), role);
		}

		return filtered;
	}

	public <S1 extends ValueDomainSubset<S1, D1>, D1 extends ValueDomain> MetadataAssertions<R, S1, D1> withDomain(S1 domain)
	{
		Set<DataStructureComponent<R, S1, D1>> components = this.components.stream()
			.filter(c -> domain.isAssignableFrom(c.getDomain()))
			.map(c -> c.asDomain(domain))
			.collect(toSet());
		
		return new MetadataAssertions<>(name, components, role, domain);
	}

	public <S1 extends ValueDomainSubset<S1, D1>, D1 extends ValueDomain> MetadataAssertions<R, S1, D1> withDomainCheck(S1 domain)
	{
		MetadataAssertions<R, S1, D1> filtered = withDomain(domain);

		if (filtered.get().size() != this.components.size())
		{
			this.components.removeAll(filtered.get());
			throw new VTLIncompatibleTypesException(name, domain, components.iterator().next());
		}

		return filtered;
	}

	public MetadataAssertions<R, S, D> withNames(Collection<String> names)
	{
		return new MetadataAssertions<>(name, names.stream()
				.flatMap(name -> components.stream()
						.filter(c -> name.equals(c.getName()))
						.findAny()
						.map(Stream::of)
						.orElse(Stream.empty()))
				.collect(toSet()), role, domain);
	}

	public MetadataAssertions<R, S, D> withNamesCheck(Collection<String> names)
	{
		MetadataAssertions<R, S, D> filtered = withNames(names);
		
		if (filtered.get().size() != this.components.size())
		{
			this.components.removeAll(filtered.get());
			throw new VTLMissingComponentsException(components, names.toArray(new String[0]));
		}
		
		return filtered;
	}

	public DataStructureComponent<R, S, D> getAsSingleton()
	{
		if (components.size() != 1)
			if (role != null)
				if (domain != null)
					throw new VTLSingletonComponentRequiredException(role, domain, components);
				else
					throw new VTLSingletonComponentRequiredException(role, components);
			else 
				throw new VTLSingletonComponentRequiredException(name, components);
		
		return components.iterator().next();
	}
}
