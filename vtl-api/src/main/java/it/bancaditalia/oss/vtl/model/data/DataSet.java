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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

import java.util.Collection;
import java.util.Iterator;
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
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerCollectors;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;

/**
 * The base interface describing a dataset
 * 
 * @author Valentino Pinna
 *
 */
public interface DataSet extends VTLValue, Iterable<DataPoint>
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
	 * @return an {@link Iterator} of this dataset's {@link DataPoint}s.
	 * The iterating order is undefined and may change on subsequent invocations.
	 */
	@Override
	public default Iterator<DataPoint> iterator()
	{
		return stream().iterator();
	}

	/**
	 * Creates a new dataset retaining the specified component along with all identifiers of this dataset
	 * @param component The component to retain.
	 * @param lineage the lineage of the membership operator
	 * 
	 * @return The projected dataset
	 */
	public DataSet membership(String component, Lineage lineage);

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
	public default DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return filter(dp -> keyValues.equals(dp.getValues(keyValues.keySet(), Identifier.class)));	
	}

	/**
	 * Creates a new DataSet by filtering this DataSet with a given {@link Predicate} on each of its {@link DataPoint}.
	 * 
	 * @param predicate The {@link Predicate} to be applied.
	 * @return A new filtered DataSet. 
	 */
	public DataSet filter(SerPredicate<DataPoint> predicate);

	/**
	 * Creates a new DataSet by transforming each of this DataSet's {@link DataPoint} by a given {@link Function}.
	 * 
	 * @param metadata The {@link DataSetMetadata structure} the new dataset must conform to.
	 * @param lineageOperator TODO
	 * @param operator a {@link Function} that maps each of this DataSet's {@link DataPoint}s.
	 * @return The new transformed DataSet. 
	 */
	public DataSet mapKeepingKeys(DataSetMetadata metadata, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator, SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator);

	/**
	 * Creates a new DataSet by joining each DataPoint of this DataSet to all indexed DataPoints of another DataSet by matching the common identifiers.
	 * 
	 * @param metadata The {@link DataSetMetadata structure} the new DataSet must conform to.
	 * @param indexed another DataSet that will be indexed and joined to each DataPoint of this DataSet.
	 * @param filter a {@link BiPredicate} used to select only a subset of the joined {@link DataPoint}s.
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @param leftJoin true if a left outer join is to be performed, false for inner join
	 * @return The new DataSet.
	 */
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet indexed, SerBiPredicate<DataPoint, DataPoint> filter, SerBinaryOperator<DataPoint> merge, boolean leftJoin);

	/**
	 * Creates a new DataSet by joining each DataPoint of this DataSet to all indexed DataPoints of another DataSet by matching the common identifiers.
	 * The same as {@code filteredMappedJoin(metadata, other, filter, merge, false)}.
	 * 
	 * @param metadata The {@link DataSetMetadata structure} the new DataSet must conform to.
	 * @param indexed another DataSet that will be indexed and joined to each DataPoint of this DataSet.
	 * @param filter a {@link BiPredicate} used to select only a subset of the joined {@link DataPoint}s.
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @return The new DataSet.
	 */
	public default DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet indexed, SerBiPredicate<DataPoint, DataPoint> filter, SerBinaryOperator<DataPoint> merge)
	{
		return filteredMappedJoin(metadata, indexed, filter, merge, false);
	}

	/**
	 * Creates a new DataSet by joining each DataPoint of this DataSet to all indexed DataPoints of another DataSet by matching the common identifiers.
	 * The same as {@code filteredMappedJoin(metadata, other, (a,  b) -> true, merge, leftJoin)}.
	 * 
	 * @param metadata The {@link DataSetMetadata structure} the new DataSet must conform to.
	 * @param indexed another DataSet that will be indexed and joined to each DataPoint of this DataSet.
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @param leftJoin true if a left outer join is to be performed, false for inner join
	 * @return The new DataSet.
	 */
	public default DataSet mappedJoin(DataSetMetadata metadata, DataSet indexed, SerBinaryOperator<DataPoint> merge, boolean leftJoin)
	{
		return filteredMappedJoin(metadata, indexed, (a,  b) -> true, merge, leftJoin);
	}

	/**
	 * Creates a new DataSet by joining each DataPoint of this DataSet to all indexed DataPoints of another DataSet by matching the common identifiers.
	 * The same as {@code filteredMappedJoin(metadata, other, (a,  b) -> true, merge)}.
	 * 
	 * @param metadata The {@link DataSetMetadata structure} the new DataSet must conform to.
	 * @param indexed another DataSet that will be indexed and joined to each DataPoint of this DataSet.
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @return The new DataSet.
	 */
	public default DataSet mappedJoin(DataSetMetadata metadata, DataSet indexed, SerBinaryOperator<DataPoint> merge)
	{
		return filteredMappedJoin(metadata, indexed, (a,  b) -> true, merge);
	}

	/**
	 * Groups all the datapoints of this DataSet having the same values for the specified identifiers, 
	 * and performs a <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#MutableReduction">mutable reduction</a>
	 * over each of a chosen subset of the groups, and applying a final transformation.
	 * 
	 * @param <T> the type of the result of the computation.
	 * @param keys the {@link Identifier}s used to group the datapoints
	 * @param filter a {@code Map} of {@link Identifier}'s values used to exclude matching groups
	 * @param groupCollector a {@link Collector} applied to each group to produce the result
	 * @param finisher a {@link BiFunction} to apply to the group key and result to produce the final result
	 * @return a {@link Stream} of {@code <T>} objects containing the result of the computation for each group. 
	 */
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter,
			SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher);
	
	/**
	 * Groups all the datapoints of this DataSet having the same values for the specified identifiers, 
	 * and performs a <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#MutableReduction">mutable reduction</a>
	 * over each of the groups, and applying a final transformation.
	 * 
	 * The same as {@link #streamByKeys(Set, Map, SerCollector, SerBiFunction)} with an empty filter.
	 * 
	 * @param <T> the type of the result of the computation.
	 * @param keys the {@link Identifier}s used to group the datapoints
	 * @param groupCollector a {@link Collector} applied to each group to produce the result
	 * @param finisher a {@link BiFunction} to apply to the group key and result to produce the final result
	 * @return a {@link Stream} of {@code <T>} objects containing the result of the computation for each group. 
	 */
	public default <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		return streamByKeys(keys, emptyMap(), groupCollector, finisher);
	}
	
	/**
	 * Groups all the datapoints of this DataSet having the same values for the specified identifiers, 
	 * and performs a <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#MutableReduction">mutable reduction</a>
	 * over each of a chosen subset of the groups.
	 * 
	 * The same as {@link #streamByKeys(Set, Map, SerCollector, SerBiFunction)} with an identity finisher.
	 *
	 * @param <T> the type of the result of the computation.
	 * @param keys the {@link Identifier}s used to group the datapoints
	 * @param filter a {@code Map} of {@link Identifier}'s values used to exclude matching groups
	 * @param groupCollector a {@link Collector} applied to each group to produce the result
	 * @return a {@link Stream} of {@code <T>} objects containing the result of the computation for each group. 
	 */
	public default <A, T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter,
			SerCollector<DataPoint, A, T> groupCollector)
	{
		return streamByKeys(keys, filter, groupCollector, (a, b) -> a);
	}
	
	/**
	 * Groups all the datapoints of this DataSet having the same values for the specified identifiers, 
	 * and performs a <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#MutableReduction">mutable reduction</a>
	 * over each of the groups.
	 * 
	 * The same as {@link #streamByKeys(Set, Map, SerCollector, SerBiFunction)} with an empty filter and an identity finisher.
	 *
	 * @param <T> the type of the result of the computation.
	 * @param keys the {@link Identifier}s used to group the datapoints
	 * @param groupCollector a {@link Collector} applied to each group to produce the result
	 * @return a {@link Stream} of {@code <T>} objects containing the result of the computation for each group. 
	 */
	public default <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, SerCollector<DataPoint, ?, T> groupCollector)
	{
		return streamByKeys(keys, emptyMap(), groupCollector);
	}
	
	/**
	 * Perform a reduction over a dataset, producing a result for each group defined common values of the specified identifiers 
	 * 
	 * @param <TT> The type of the result of the aggregation 
	 * @param structure the metadata of the structure produced
	 * @param keys the identifiers on whose values datapoints should be grouped 
	 * @param groupCollector the aggregator that performs the reduction
	 * @param finisher a finisher that may manipulate the result given the group where it belongs
	 * 
	 * @return a new dataset where each datapoint is the result of the aggregation of a group.
	 */
	public <TT> DataSet aggr(DataSetMetadata structure, Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			SerCollector<DataPoint, ?, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> finisher);
	
	public <TT> DataSet analytic(Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components, 
			WindowClause clause, Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors, 
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers);

	public default <TT> DataSet analytic(DataStructureComponent<?, ?, ?> measure, 
			WindowClause clause, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT> collector, 
			SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>> finisher)
	{
		return analytic(singletonMap(measure, measure), clause, singletonMap(measure, collector), singletonMap(measure, finisher));
	}
	
	public default <TT> DataSet analytic(Set<? extends DataStructureComponent<?, ?, ?>> components, WindowClause clause,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors, 
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers)
	{
		return analytic(components.stream().collect(SerCollectors.toMapWithValues(k -> k)), clause, collectors, finishers);
	}

	public default DataSet analytic(Set<? extends DataStructureComponent<?, ?, ?>> components, WindowClause clause,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>>> collectors)
	{
		Map<? extends DataStructureComponent<?, ?, ?>, DataStructureComponent<?, ?, ?>> measures = components.stream()
				.collect(toMapWithValues(measure -> measure));
		Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<ScalarValue<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers = components.stream()
				.collect(toMapWithValues(measure -> (value, originalValue) -> singleton(value)));
		
		return analytic(measures, clause, collectors, finishers);
	}

	/**
	 * Creates a new DataSet as the union of this and other datasets.
	 * The datasets must have the same structure, and duplicated datapoints are taken from the leftmost operand
	 * 
	 * @param others The datasets to perform the union with
	 * @return The result of the union. 
	 */
	public DataSet union(DataSet... others);

	/**
	 * Creates a new dataset as containing all the datapoints of this dataset that don't have the same identifiers as the ones in the other dataset.
	 * 
	 * @param other the other dataset
	 * @return the set difference between the two datasets.
	 */
	public DataSet setDiff(DataSet other);

	/**
	 * Obtains a component with given name, and checks that it belongs to the specified domain.
	 * 
	 * @param name The requested component's name.
	 * @param domain A non-null instance of a domain.
	 * @return An {@link Optional} containing the requested component if it exists.
	 * 
	 * @throws NullPointerException if domain is null.
	 */
	public default <S extends ValueDomainSubset<S, D>, D extends ValueDomain> Optional<DataStructureComponent<?, S, D>> getComponent(String name, S domain)
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
	public default <R extends ComponentRole> Optional<DataStructureComponent<R, ?, ?>> getComponent(String name, Class<R> role)
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
	public default <R extends ComponentRole, S extends ValueDomainSubset<S, D>, D extends ValueDomain> Optional<DataStructureComponent<R, S, D>> getComponent(String name,
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
		try (Stream<DataPoint> stream = stream())
		{
			return stream.count();
		}
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
	public default <R extends ComponentRole, S extends ValueDomainSubset<S, D>, D extends ValueDomain> Set<DataStructureComponent<R, S, D>> getComponents(Class<R> role,
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

	/**
	 * Checks if a DataPoint is not contained in this DataSet.
	 * 
	 * <b>NOTE</b>: The default implementation performs a linear search, potentially traversing this DataSet entirely.
	 */
	public default boolean notContains(DataPoint datapoint)
	{
		return !getMatching(datapoint.getValues(Identifier.class)).stream().findAny().isPresent();
	}
	
	/**
	 * @return true if this DataSet can be cached
	 */
	public default boolean isCacheable()
	{
		return true;
	}
}
