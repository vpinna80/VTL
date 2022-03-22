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
package it.bancaditalia.oss.vtl.model.data;

import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

/**
 * The immutable representation of the data structure of a {@link DataSet}.
 * 
 * @author Valentino Pinna
 *
 */
public interface DataSetMetadata extends Set<DataStructureComponent<?, ?, ?>>, VTLValueMetadata, Serializable
{
	/**
	 * Queries for components of this {@link DataSetMetadata} having a specified role.
	 * 
	 * The returned set should not be altered in any way.
	 * 
	 * @param <R> the role type
	 * @param role the role to query
	 * @return A set of the queried components.
	 */
	public <R extends ComponentRole> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> role);

	/**
	 * Queries for components of this {@link DataSetMetadata} having a specified role and domain.
	 * 
	 * The returned set should not be altered in any way.
	 * 
	 * @param <R> the role type
	 * @param <S> the domain subset type
	 * @param <D> the domain type
	 * @param role the role to query
	 * @param domain the domain to query
	 * @return A set of the queried components.
	 */
	public default <R extends ComponentRole, S extends ValueDomainSubset<S, D>, D extends ValueDomain> Set<DataStructureComponent<R, S, D>> getComponents(Class<R> role, S domain)
	{
		return getComponents(role).stream()
				.filter(c -> domain.isAssignableFrom(c.getDomain()))
				.map(c -> c.asRole(role).asDomain(domain))
				.collect(toSet());
	}

	/**
	 * Checks if this {@link DataSetMetadata} contains a component with the specified name.
	 * 
	 * @param name the name of the desired component
	 * @return true if this {@link DataSetMetadata} contains a component with the specified name.
	 */
	public boolean contains(String name);

	/**
	 * Queries this {@link DataSetMetadata} for a component with the specified name.
	 * 
	 * @param name the name of the desired component
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public Optional<DataStructureComponent<?, ?, ?>> getComponent(String name);

	/**
	 * Queries this {@link DataSetMetadata} for a component with the specified name and value domain.
	 * 
	 * @param <S> the domain subset type
	 * @param <D> the domain type
	 * @param name the name of the desired component
	 * @param domain the domain to query
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public default <S extends ValueDomainSubset<S, D>, D extends ValueDomain> Optional<DataStructureComponent<?, S, D>> getComponent(String name, S domain)
	{
		return getComponent(name)
				.filter(c -> domain.isAssignableFrom(c.getDomain()))
				.map(c -> c.asDomain(domain));
	}

	/**
	 * Queries this {@link DataSetMetadata} for a component with the specified name and role.
	 * 
	 * @param <R> the role type
	 * @param name the name of the desired component
	 * @param role the role to query
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public default <R extends ComponentRole> Optional<DataStructureComponent<R, ?, ?>> getComponent(String name, Class<R> role)
	{
		return getComponent(name)
				.filter(c -> c.is(role))
				.map(c -> c.asRole(role));
	}

	/**
	 * Queries this {@link DataSetMetadata} for a component with the specified name, role and value domain.
	 * 
	 * @param <R> the role type
	 * @param <S> the domain subset type
	 * @param <D> the domain type
	 * @param name the name of the desired component
	 * @param role the role to query
	 * @param domain the domain to query
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public default <R extends ComponentRole, S extends ValueDomainSubset<S, D>, D extends ValueDomain> Optional<DataStructureComponent<R, S, D>> getComponent(String name, Class<R> role, S domain)
	{
		return getComponent(name)
				.filter(c -> domain.isAssignableFrom(c.getDomain()))
				.filter(c -> c.is(role))
				.map(c -> c.asDomain(domain))
				.map(c -> c.asRole(role));
	}

	/**
	 * Queries this {@link DataSetMetadata} for components with the specified names.
	 *
	 * The returned set should not be altered in any way.
	 * 
	 * @param names the names to query
	 * @return a set containing the existing components.
	 * @throws VTLMissingComponentsException if one of the names isn't found.
	 */
	public default Set<DataStructureComponent<?, ?, ?>> getComponents(String... names)
	{
		return Arrays.stream(names)
				.map(this::getComponent)
				.map(o -> o.orElseThrow(() -> new VTLMissingComponentsException(this, names)))
				.collect(toSet());
	}

	/**
	 * Queries this {@link DataSetMetadata} for components with the specified names.
	 *
	 * The returned set should not be altered in any way.
	 * 
	 * @param names the names to query
	 * @return a set containing the existing components.
	 * @throws VTLMissingComponentsException if one of the names isn't found.
	 */
	public default Set<DataStructureComponent<?, ?, ?>> getComponents(Collection<String> names)
	{
		return getComponents(names.toArray(new String[0]));
	}

	/**
	 * Queries this {@link DataSetMetadata} for components having the specified role and matching each of the specified names.
	 *
	 * The returned set should not be altered in any way.
	 * 
	 * @param <R> the role type
	 * @param names the names to query
	 * @param role the role to query
	 * @return a set containing the existing components.
	 * @throws VTLMissingComponentsException if one of the names isn't found.
	 */
	public default <R extends ComponentRole> Set<DataStructureComponent<R, ?, ?>> getComponents(Collection<String> names, Class<R> role)
	{
		return getComponents(names.toArray(new String[0])).stream()
				.filter(c -> c.is(role))
				.map(c -> c.asRole(role))
				.collect(toSet());
	}

	/**
	 * Creates a new structure subspacing this structure over the provided identifiers.
	 * 
	 * @param subspace the identifiers that must be subspaced
	 * @return The new structure.
	 */
	public DataSetMetadata subspace(Collection<? extends DataStructureComponent<Identifier, ?, ?>> subspace);
	
	/**
	 * Creates a new structure where only the named non-identifier components are kept.
	 * 
	 * @param names names of the non-identifier components to keep
	 * @return The new structure.
	 */
	public DataSetMetadata keep(String... names);
	
	/**
	 * Creates a new structure dropping all the named non-identifier components.
	 * 
	 * @param names names of the non-identifier components to drop
	 * @return The new structure.
	 */
	public DataSetMetadata drop(Collection<String> names);

	/**
	 * Creates a new structure by performing a VTL membership operation on this structure.
	 * 
	 * @param name the name of the component on which the membership operation is performed
	 * @return The new structure.
	 */
	public DataSetMetadata membership(String name);

	/**
	 * Creates a new structure by joining this and another {@link DataSetMetadata}.
	 *  
	 * @param other the other structure to join
	 * @return the new structure.
	 */
	public DataSetMetadata joinForOperators(DataSetMetadata other);

	/**
	 * Creates a new structure by renaming a component of this {@link DataSetMetadata}.
	 * 
	 * If a component with the new name already exists, the behaviour is undefined.
	 * 
	 * @param component the component to rename
	 * @param newName the new name for the component
	 * @return the new structure.
	 */
	public DataSetMetadata rename(DataStructureComponent<?, ?, ?> component, String newName);

	/**
	 * Creates a new structure by pivoting the specified measure over an identifier that is defined on a string enumerated domain (codelist).
	 * 
	 * @param <S> the domain subset type of the measure
	 * @param <D> the domain type of the measure
	 * @param identifier the identifier
	 * @param measure the measure
	 * @return the new structure.
	 */
	public <S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataSetMetadata pivot(DataStructureComponent<Identifier, ? extends StringEnumeratedDomainSubset<?, ?, ?, ?>, StringDomain> identifier, DataStructureComponent<Measure, S, D> measure);
}
