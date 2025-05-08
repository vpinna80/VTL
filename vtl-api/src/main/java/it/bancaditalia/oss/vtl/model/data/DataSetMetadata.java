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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
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
	public <R extends Component> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> role);

	/**
	 * Queries for identifiers of this {@link DataSetMetadata}.
	 * 
	 * The returned set should not be altered in any way.
	 * @return A set of the queried identifiers.
	 */
	public default Set<DataStructureComponent<Identifier, ?, ?>> getIDs()
	{
		return getComponents(Identifier.class);
	}

	/**
	 * Queries for measures of this {@link DataSetMetadata}.
	 * 
	 * The returned set should not be altered in any way.
	 * @return A set of the queried measures.
	 */
	public default Set<DataStructureComponent<Measure, ?, ?>> getMeasures()
	{
		return getComponents(Measure.class);
	}

	/**
	 * Queries for components of this {@link DataSetMetadata} having a specified role and domain.
	 * 
	 * The returned set should not be altered in any way.
	 * 
	 * @param <R> the role type
	 * @param role the role to query
	 * @param domain the domain to query
	 * @return A set of the queried components.
	 */
	@SuppressWarnings("unchecked")
	public default <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> Set<DataStructureComponent<R, S, D>> getComponents(Class<R> role, S domain)
	{
		return getComponents(role).stream()
				.filter(c -> domain.isAssignableFrom(c.getVariable().getDomain()))
				.map(c -> c.asRole(role))
				.map(c -> (DataStructureComponent<R, S, D>) c)
				.collect(toSet());
	}

	/**
	 * Checks if this {@link DataSetMetadata} contains a component with the specified name.
	 * 
	 * @param name the name of the desired component
	 * @return true if this {@link DataSetMetadata} contains a component with the specified name.
	 */
	public boolean contains(VTLAlias name);

	/**
	 * Queries this {@link DataSetMetadata} for a component with the specified name.
	 * 
	 * @param name the name of the desired component
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public Optional<DataStructureComponent<?, ?, ?>> getComponent(VTLAlias name);

	/**
	 * Queries this {@link DataSetMetadata} for a component with the specified name and value domain.
	 * 
	 * @param name the name of the desired component
	 * @param domain the domain to query
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public default Optional<DataStructureComponent<?, ?, ?>> getComponent(VTLAlias name, ValueDomainSubset<?, ?> domain)
	{
		return getComponent(name)
				.filter(c -> domain.isAssignableFrom(c.getVariable().getDomain()));
	}

	/**
	 * Queries this {@link DataSetMetadata} for a component with the specified name and role.
	 * 
	 * @param <R> the role type
	 * @param name the name of the desired component
	 * @param role the role to query
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public default <R extends Component> Optional<DataStructureComponent<R, ?, ?>> getComponent(VTLAlias name, Class<R> role)
	{
		return getComponent(name)
				.filter(c -> c.is(role))
				.map(c -> c.asRole(role));
	}

	/**
	 * Queries this {@link DataSetMetadata} for a component with the specified name, role and value domain.
	 * 
	 * @param <R> the role type
	 * @param name the name of the desired component
	 * @param role the role to query
	 * @param domain the domain to query
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public default <R extends Component> Optional<DataStructureComponent<R, ?, ?>> getComponent(VTLAlias name, Class<R> role, ValueDomainSubset<?, ?> domain)
	{
		return getComponent(name)
				.filter(c -> domain.isAssignableFrom(c.getVariable().getDomain()))
				.filter(c -> c.is(role))
				.map(c -> c.asRole(role));
	}

	/**
	 * Creates a new structure subspacing this structure over the provided identifiers.
	 * 
	 * @param subspace the identifiers that must be subspaced
	 * @return The new structure.
	 */
	public DataSetMetadata subspace(Collection<? extends DataStructureComponent<Identifier, ?, ?>> subspace);

	/**
	 * Creates a new structure by performing a VTL membership operation on this structure.
	 * 
	 * @param name the name of the component on which the membership operation is performed
	 * @return The new structure.
	 */
	public DataSetMetadata membership(VTLAlias name);

	/**
	 * Creates a new structure by joining this and another {@link DataSetMetadata}.
	 *  
	 * @param other the other structure to join
	 * @return the new structure.
	 */
	public DataSetMetadata joinForOperators(DataSetMetadata other);

	/**
	 * Extracts a singleton component of a given role if it exists in the current structure, or throws an exception otherwise.
	 * 
	 * @param <R> The role type
	 * @param role The class representing the role
	 * @return the singleton component of given role
	 */
	public default <R extends Component> DataStructureComponent<R, ?, ?> getSingleton(Class<R> role)
	{
		Set<DataStructureComponent<R, ?, ?>> set = getComponents(role);
		if (set.size() != 1)
			throw new VTLSingletonComponentRequiredException(role, set);
		
		return set.iterator().next();
	}

	/**
	 * Extracts a singleton component of a given role that is assignable to the given domain if it exists in this structure, or throws an exception otherwise.
	 * 
	 * @param <R> The role type
	 * @param <S> the domain subset type of the component
	 * @param <D> the domain type of the component
	 * @param role The class representing the role
	 * @param role The valuedomain subset which the component must be assignable to
	 * @return the singleton component
	 */
	public default <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataStructureComponent<R, S, D> getSingleton(Class<R> role, S domain)
	{
		Set<DataStructureComponent<R, S, D>> set = getComponents(role, domain);
		if (set.size() != 1)
			throw new VTLSingletonComponentRequiredException(role, domain ,set);
		
		return set.iterator().next();
	}
	
	public default Set<DataStructureComponent<Identifier, ?, ?>> matchIdComponents(Collection<? extends VTLAlias> names, String operation)
	{
		if (names == null || names.isEmpty())
			return new HashSet<>(getIDs());
		
		return names.stream()
			.peek(n -> { if (!contains(n)) throw new VTLMissingComponentsException(this, n); })
			.map(this::getComponent)
			.map(Optional::get)
			.peek(c -> { if (!c.is(Identifier.class)) throw new VTLIncompatibleRolesException(operation, c, Identifier.class); })
			.map(c -> c.asRole(Identifier.class))
			.collect(toSet());
	}
	
	@Override
	public default boolean isDataSet()
	{
		return true;
	}
}
