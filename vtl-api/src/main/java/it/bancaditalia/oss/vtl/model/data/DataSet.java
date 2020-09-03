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
import java.util.stream.Collector;
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
	/**
	 * @return The {@link DataSetMetadata structure} of this DataSet.
	 */
	@Override
	public DataSetMetadata getMetadata();
	
	/**
	 * @return a {@link Stream} of this dataset's {@link DataPoint}s.
	 * The stream does not conform to a particular ordering.
	 */
	public Stream<DataPoint> stream();

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
	 * Create a new DataSet by filtering this DataSet's {@link DataPoint}s matching the specified values for some identifiers.
	 * 
	 * @param keyValues A {@link Map} containing values for some of this DataSet {@link Identifier}s.
	 *      If the map is empty, the result is this {@code DataSet}.
	 * @return A new {@code DataSet} of matching {@link DataPoint}s, eventually empty.
	 */
	public default DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues)
	{
		return filter(dp -> keyValues.equals(dp.getValues(keyValues.keySet(), Identifier.class)));	
	}

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
	 * @param metadata The {@link DataSetMetadata structure} the new dataset must conform to.
	 * @param operator a {@link Function} that maps each of this DataSet's {@link DataPoint}s.
	 * @return The new transformed DataSet. 
	 */
	public DataSet mapKeepingKeys(DataSetMetadata metadata, Function<? super DataPoint, ? extends Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>>> operator);

	/**
	 * Creates a new DataSet by joining DataPoints of this and another DataSet by the common identifiers.
	 * 
	 * @param metadata The {@link DataSetMetadata structure} the new DataSet must conform to.
	 * @param other another DataSet that will be joined to this DataSet.
	 * @param filter a {@link BiPredicate} used to select only a subset of the joined {@link DataPoint}s.
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @return The new DataSet.
	 */
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, BiPredicate<DataPoint,DataPoint> filter, BinaryOperator<DataPoint> merge);

	/**
	 * Creates a new DataSet by joining DataPoints of this and another DataSet by the common identifiers.
	 * The same as {@code filteredMappedJoin(metadata, other, (a,  b) -> true, merge)}.
	 * 
	 * @param metadata The {@link DataSetMetadata structure} the new DataSet must conform to.
	 * @param other another DataSet that will be joined to this DataSet. 
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @return The new DataSet.
	 */
	public default DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, BinaryOperator<DataPoint> merge)
	{
		return filteredMappedJoin(metadata, other, (a,  b) -> true, merge);
	}

	/**
	 * Perform a computation on each of the groups of the {@link DataPoint}s of this DataSet which 
	 * have the same value for each of (a subset of) its {@link Identifier}s.
	 * 
	 * The same as {@code streamByKeys(keys, Collections.emptyMap(), groupMapper)}.
	 * 
	 * @param <T> the type of the result of the computation.
	 * @param keys the {@link Identifier}s used to group the keys.
	 * @param groupCollector a {@link Collector} applied to each group to produce the final result
	 * @return a {@link Stream} of {@code <T>} objects containing the result of the computation for each group. 
	 */
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter,
			Collector<DataPoint, A, TT> groupCollector,
			BiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, T> finisher);
	
	public default <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			Collector<DataPoint, A, TT> groupCollector,
			BiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, T> finisher)
	{
		return streamByKeys(keys, emptyMap(), groupCollector, finisher);
	}
	
	public default <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter,
			Collector<DataPoint, ?, T> groupCollector)
	{
		return streamByKeys(keys, filter, groupCollector, (a, b) -> a);
	}
	
	public default <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, Collector<DataPoint, ?, T> groupCollector)
	{
		return streamByKeys(keys, emptyMap(), groupCollector);
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
		return getMetadata().getComponent(name, domain);
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
		return getMetadata().getComponent(name, role);
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
		return getMetadata().getComponent(name, role, domain);
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
	 * @see DataSetMetadata#getComponents(Class) 
	 */
	public default <R extends ComponentRole> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
	{
		return getMetadata().getComponents(typeOfComponent);
	}

	/**
	 * @see DataSetMetadata#getComponents(Class, ValueDomainSubset) 
	 */
	public default <R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> Set<DataStructureComponent<R, S, D>> getComponents(Class<R> role,
			S domain)
	{
		return getMetadata().getComponents(role, domain);
	}

	/**
	 * Checks if a DataPoint is contained in this DataSet.
	 * 
	 * <b>NOTE</b>: The default implementation performs a linear search, potentially traversing this DataSet entirely.
	 */
	public default boolean contains(DataPoint datapoint)
	{
		return getMatching(datapoint.getValues(Identifier.class)).stream().findAny().isPresent();
	}

	public default boolean notContains(DataPoint datapoint)
	{
		return !getMatching(datapoint.getValues(Identifier.class)).stream().findAny().isPresent();
	}
}
