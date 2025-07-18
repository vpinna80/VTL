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

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;

/**
 * The immutable representation of the data structure of a {@link DataSet}.
 * 
 * @author Valentino Pinna
 *
 */
public interface DataStructureDefinition extends Set<DataStructureComponent<?>>, Serializable
{
	/**
	 * @return The alias of this data structure definition
	 */
	public VTLAlias getAlias();
	
	/**
	 * Queries for components of this {@link DataStructureDefinition} having a specified role.
	 * 
	 * The returned set should not be altered in any way.
	 * 
	 * @param <R> the role type
	 * @param role the role to query
	 * @return A set of the queried components.
	 */
	public <R extends Component> Set<DataStructureComponent<R>> getComponents(Class<R> role);

	/**
	 * Queries for identifiers of this {@link DataStructureDefinition}.
	 * 
	 * The returned set should not be altered in any way.
	 * @return A set of the queried identifiers.
	 */
	public default Set<DataStructureComponent<Identifier>> getIDs()
	{
		return getComponents(Identifier.class);
	}

	/**
	 * Queries for measures of this {@link DataStructureDefinition}.
	 * 
	 * The returned set should not be altered in any way.
	 * @return A set of the queried measures.
	 */
	public default Set<DataStructureComponent<Measure>> getMeasures()
	{
		return getComponents(Measure.class);
	}

	/**
	 * Checks if this {@link DataStructureDefinition} contains a component with the specified name.
	 * 
	 * @param name the name of the desired component
	 * @return true if this {@link DataStructureDefinition} contains a component with the specified name.
	 */
	public boolean contains(VTLAlias name);

	/**
	 * Queries this {@link DataStructureDefinition} for a component with the specified name.
	 * 
	 * @param alias the alias of the desired component
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public Optional<DataStructureComponent<?>> getComponent(VTLAlias alias);

	/**
	 * Queries this {@link DataStructureDefinition} for a component with the specified name and role.
	 * 
	 * @param <R> the role type
	 * @param alias the alias of the desired component
	 * @param role the role to query
	 * @return an {@link Optional} containing the component if one exists.
	 */
	public default <R extends Component> Optional<DataStructureComponent<R>> getComponent(VTLAlias alias, Class<R> role)
	{
		return getComponent(alias)
				.filter(c -> c.is(role))
				.map(c -> c.asRole(role));
	}
//
//	/**
//	 * Creates a new structure subspacing this structure over the provided identifiers.
//	 * 
//	 * @param subspace the identifiers that must be subspaced
//	 * @return The new structure.
//	 */
//	public DataStructureDefinition subspace(Collection<? extends DataStructureComponent<Identifier>> subspace);
//
//	/**
//	 * Creates a new structure by performing a VTL membership operation on this structure.
//	 * 
//	 * @param name the name of the component on which the membership operation is performed
//	 * @return The new structure.
//	 */
//	public DataStructureDefinition membership(VTLAlias name);
//
//	/**
//	 * Creates a new structure by joining this and another {@link DataStructureDefinition}.
//	 *  
//	 * @param other the other structure to join
//	 * @return the new structure.
//	 */
//	public DataStructureDefinition joinForOperators(DataStructureDefinition other);
}
