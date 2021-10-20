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

import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.entryByKeyValue;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.SerCollectors;
import it.bancaditalia.oss.vtl.util.Utils;

/**
 * An immutable representation of a datapoint of a VTL dataset.
 * 
 * @author Valentino Pinna
 */
public interface DataPoint extends Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, Serializable
{
	/**
	 * Defines a {@link Comparator} that enforces an ordering using the values of a given component
	 * 
	 * @param component the component whose values are used for the ordering
	 * @return the Comparator instance
	 */
	public static <S extends ValueDomainSubset<S, D>, D extends ValueDomain> Comparator<DataPoint> compareBy(DataStructureComponent<Identifier, S, D> component)
	{
		return (Comparator<DataPoint> & Serializable) (dp1, dp2) -> dp1.getValue(component).compareTo(dp2.getValue(component));
	}

	/**
	 * Creates a new datapoint dropping all provided non-id components
	 * 
	 * @param components the components to drop
	 * @return a new datapoint without the provided components.
	 */
	public DataPoint dropComponents(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> components);

	/**
	 * Creates a new datapoint keeping all the identifiers and only the provided non-id components
	 * 
	 * @param components the components to drop
	 * @return a new datapoint with all existing ids and the provided components.
	 */
	public DataPoint keep(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> components);

	/**
	 * Creates a new datapoint renaming the provided component to another one with the same role.
	 *
	 * @param oldComponent the component to be renamed
	 * @param newComponent the already renamed component
	 * @return a new datapoint with the old component renamed.
	 */
	public DataPoint renameComponent(DataStructureComponent<?, ?, ?> oldComponent, DataStructureComponent<?, ?, ?> newComponent);

	/**
	 * Create a new datapoint combining this and another datapoint.
	 * All existing components keep their values in this datapoint and aren't updated with new values.
	 * @param transformation The transformation that originated the combine operation
	 * @param other the datapoint to combine with this datapoint
	 * 
	 * @return a new datapoint that is the combination of this and another datapoint.
	 */
	public DataPoint combine(Transformation transformation, DataPoint other);
	
	/**
	 * If the component exists, retrieves the value for it in this datapoint, performing a cast of the result.
	 * 
	 * @param <S> the domain subset type of the component
	 * @param <D> the domain type of the component
	 * @param component the component to query
	 * @return the casted value for the component if exists.
	 * @throws NullPointerException if the component is not found
	 */
	public default <S extends ValueDomainSubset<S, D>, D extends ValueDomain> ScalarValue<?, ?, S, D> getValue(DataStructureComponent<?, S, D> component)
	{
		return component.getDomain().cast(get(component));
	}
	
	/**
	 * Checks if this datapoint identifiers' values match all the provided ones.
	 * 
	 * @param identifierValues the id values to check
	 * @return true if this datapoint matches the provided identifiers' values.
	 */
	public default boolean matches(Map<? extends DataStructureComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> identifierValues)
	{
		return !Utils.getStream(identifierValues)
				.filter(entryByKeyValue((k, v) -> !get(k).equals(k.cast(v))))
				.findAny()
				.isPresent();
	}

	/**
	 * Query all values for components having the specified role.
	 * 
	 * @param <R> the component role type
	 * @param role role of the components
	 * @return a map with values for each component of the specified role.
	 */
	public <R extends ComponentRole> Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role);

	/**
	 * Returns the values for multiple components.
	 * 
	 * @param components the collection of components to query
	 * @return a map with the values for the specified components.
	 */
	public default Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return Utils.getStream(components)
				.filter(this::containsKey)
				.collect(SerCollectors.toMapWithValues(this::get));
	}

	/**
	 * Returns the values for multiple components having a specified role and matching one of the specified names.
	 * 
	 * @param <R> the component role type
	 * @param role role of the components
	 * @param names collection of names
	 * @return a map with the values for all the components having the specified role and matching one of the specified names.
	 */
	public default <R extends ComponentRole> Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role, Collection<String> names)
	{
		return Utils.getStream(keySet())
				.map(c -> new SimpleEntry<>(c, c.getName()))
				.filter(entryByValue(names::contains))
				.map(Entry::getKey)
				.filter(c -> c.is(role))
				.map(c -> c.as(role))
				.collect(SerCollectors.toMapWithValues(this::get));
	}

	/**
	 * Returns the values for the chosen components having a specified role.
	 * 
	 * @param <R> the component role type
	 * @param role role of the components
	 * @param components collection of components to query
	 * @return a map with the values for chosen the components having the specified role.
	 */
	public default <R extends ComponentRole> Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Collection<? extends DataStructureComponent<R, ?, ?>> components, Class<R> role)
	{
		return Utils.getStream(getValues(role).entrySet())
				.filter(entryByKey(components::contains))
				.map(keepingValue(c -> c.as(role)))
				.collect(SerCollectors.entriesToMap());
	}

	/**
	 * Get the source transformation of this DataPoint
	 * 
	 * @return the transformation from where the datapoint originated 
	 */
	public Lineage getLineage();
}
