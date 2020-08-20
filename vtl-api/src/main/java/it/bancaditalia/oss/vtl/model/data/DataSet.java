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
package it.bancaditalia.oss.vtl.model.data;

import static java.util.Collections.emptyMap;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;

/**
 * The base interface describing a dataset
 * 
 * @author Valentino Pinna
 *
 */
public interface DataSet extends VTLValue
{
	@Override
	public VTLDataSetMetadata getMetadata();
	
	/**
	 * @return a {@link Stream} of this dataset's {@link DataPoint}s.
	 * The stream does not conform to a particular ordering.
	 */
	public Stream<DataPoint> stream();

	/**
	 * @return The {@link VTLDataSetMetadata structure} of this DataSet.
	 */
	public VTLDataSetMetadata getDataStructure();

	/**
	 * Creates a new dataset retaining the specified component along with all identifiers of this dataset
	 * @param component The component to retain.
	 * 
	 * @return The projected dataset
	 */
	public DataSet membership(String component);

	/**
	 * Finds a component with given name
	 * 
	 * @param name The requested component's name.
	 * @return an {@link Optional} eventually containing the requested {@link DataStructureComponent} if one was found.
	 */
	public Optional<DataStructureComponent<?, ?, ?>> getComponent(String name);

	/**
	 * Get a subset of this DataSet {@link DataPoint}s that match the specified values for some identifiers.
	 * 
	 * @param keyValues A {@link Map} containing values for some of this DataSet {@link Identifier}s.
	 *      If the map is empty, the result is the same as {@link #stream()}.
	 * @return A {@link Stream} of matching {@link DataPoint}s, eventually empty.
	 */
	public Stream<DataPoint> getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues);

	/**
	 * Creates a new DataSet by filtering this DataSet with a given {@link Predicate} on each of its {@link DataPoint}.
	 * 
	 * @param predicate The {@link Predicate} to be applied.
	 * @return A new filtered DataSet. 
	 */
	public DataSet filter(Predicate<DataPoint> predicate);

	/**
	 * Creates a new DataSet by transforming each of this DataSet's {@link DataPoint} by a given {@link Function}.
	 * 
	 * @param metadata The {@link VTLDataSetMetadata structure} the new dataset must conform to.
	 * @param operator a {@link Function} that maps each of this DataSet's {@link DataPoint}s.
	 * @return The new transformed DataSet. 
	 */
	public DataSet mapKeepingKeys(VTLDataSetMetadata metadata, Function<? super DataPoint, ? extends Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>>> operator);

	/**
	 * Creates a new DataSet by joining DataPoints of this and another DataSet by the common identifiers.
	 * 
	 * @param metadata The {@link VTLDataSetMetadata structure} the new DataSet must conform to.
	 * @param other another DataSet that will be joined to this DataSet.
	 * @param filter a {@link BiPredicate} used to select only a subset of the joined {@link DataPoint}s.
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @return The new DataSet.
	 */
	public DataSet filteredMappedJoin(VTLDataSetMetadata metadata, DataSet other, BiPredicate<DataPoint,DataPoint> filter, BinaryOperator<DataPoint> merge);

	/**
	 * Creates a new DataSet by joining DataPoints of this and another DataSet by the common identifiers.
	 * The same as {@code filteredMappedJoin(metadata, other, (a,  b) -> true, merge)}.
	 * 
	 * @param metadata The {@link VTLDataSetMetadata structure} the new DataSet must conform to.
	 * @param other another DataSet that will be joined to this DataSet. 
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @return The new DataSet.
	 */
	public default DataSet filteredMappedJoin(VTLDataSetMetadata metadata, DataSet other, BinaryOperator<DataPoint> merge)
	{
		return filteredMappedJoin(metadata, other, (a,  b) -> true, merge);
	}

	/**
	 * Perform a computation on each of the groups of the {@link DataPoint}s of this DataSet which 
	 * have the same value for each of (a subset of) its {@link Identifier}s.
	 * 
	 * @param <T> the type of the result of the computation.
	 * @param keys the {@link Identifier}s used to group the keys.
	 * @param filter a {@link Map} containing values for some of this DataSet {@link Identifier}s.
	 *      The map will be used to filter out groups from the final computation. 
	 * @param groupMapper a {@link BiFunction} applied to each group to produce the final result
	 * @return a {@link Stream} of {@code <T>} objects containing the result of the computation for each group. 
	 */
	public <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter,
			BiFunction<? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, ? super Stream<DataPoint>, T> groupMapper);

	/**
	 * Perform a computation on each of the groups of the {@link DataPoint}s of this DataSet which 
	 * have the same value for each of (a subset of) its {@link Identifier}s.
	 * 
	 * The same as {@code streamByKeys(keys, Collections.emptyMap(), groupMapper)}.
	 * 
	 * @param <T> the type of the result of the computation.
	 * @param keys the {@link Identifier}s used to group the keys.
	 * @param groupMapper a {@link BiFunction} applied to each group to produce the final result
	 * @return a {@link Stream} of {@code <T>} objects containing the result of the computation for each group. 
	 */
	public default <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			BiFunction<? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, ? super Stream<DataPoint>, T> groupMapper)
	{
		return streamByKeys(keys, emptyMap(), groupMapper);
	}
	
	/**
	 * Obtains multiple components with given names
	 * 
	 * @param names The requested components' names.
	 * @return The requested components, or an empty set if none was found.
	 */
	public default Set<DataStructureComponent<?, ?, ?>> getComponents(String... names)
	{
		return getDataStructure().getComponents(names);
	}

	/**
	 * Obtains a component with given name, and checks that it belongs to the specified domain.
	 * 
	 * @param name The requested component's name.
	 * @param domain A non-null instance of a domain.
	 * @return The requested component, or null if no one was found.
	 * 
	 * @throws NullPointerException if domain is null.
	 */
	public default <S extends ValueDomainSubset<D>, D extends ValueDomain> DataStructureComponent<?, S, D> getComponent(String name, S domain)
	{
		return getDataStructure().getComponent(name, domain);
	}


	/**
	 * Obtains a component with given name, and checks that it belongs to the specified domain.
	 * 
	 * @param name The requested component's name.
	 * @param role The role of component desired (Measure, Identifier, Attribute).
	 * @return The requested component, or null if no one was found.
	 * 
	 * @throws NullPointerException if domain is null.
	 */
	public default <R extends ComponentRole> DataStructureComponent<R, ?, ?> getComponent(String name, Class<R> role)
	{
		return getDataStructure().getComponent(name, role);
	}
	
	/**
	 * Obtains a component with given name if it has the specified role, and checks that it belongs to the specified domain.
	 * 
	 * @param name The requested component's name.
	 * @param role The role of component desired (Measure, Identifier, Attribute).
	 * @param domain A non-null instance of a domain.
	 * @return The requested component, or null if no one was found.
	 * 
	 * @throws NullPointerException if domain is null.
	 */
	public default <R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> DataStructureComponent<R, S, D> getComponent(String name,
			Class<R> role, S domain)
	{
		return getDataStructure().getComponent(name, role, domain);
	}
	
	/**
	 * <b>NOTE</b>: The default implementation traverses this DataSet entirely.
	 * 
	 * @return The size of this DataSet.
	 */
	public default long size()
	{
		return stream().count();
	}

	/**
	 * @see VTLDataSetMetadata#getComponents(Class) 
	 */
	public default <R extends ComponentRole> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
	{
		return getDataStructure().getComponents(typeOfComponent);
	}

	/**
	 * @see VTLDataSetMetadata#getComponents(Class, ValueDomainSubset) 
	 */
	public default <R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> Set<DataStructureComponent<R, S, D>> getComponents(Class<R> role,
			S domain)
	{
		return getDataStructure().getComponents(role, domain);
	}

	/**
	 * Checks if a DataPoint is contained in this DataSet.
	 * 
	 * <b>NOTE</b>: The default implementation performs a linear search, potentially traversing this DataSet entirely.
	 */
	public default boolean contains(DataPoint datapoint)
	{
		return getMatching(datapoint.getValues(Identifier.class)).findAny().isPresent();
	}

	public default boolean notContains(DataPoint datapoint)
	{
		return !getMatching(datapoint.getValues(Identifier.class)).findAny().isPresent();
	}
}
