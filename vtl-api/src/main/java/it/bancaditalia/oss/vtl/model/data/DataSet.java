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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.domain.StringCodeList;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;

public interface DataSet extends VTLValue
{
	public static interface VTLDataSetMetadata extends VTLValueMetadata, DataStructure
	{
		public void registerIndex(Set<? extends DataStructureComponent<Identifier, ?, ?>> keys);
		
		public Set<Set<? extends DataStructureComponent<Identifier, ?, ?>>> getRequestedIndexes();

		@Override
		public VTLDataSetMetadata swapComponent(DataStructureComponent<?, ?, ?> oldComponent, DataStructureComponent<?, ?, ?> newComponent);
		
		@Override
		public VTLDataSetMetadata keep(String... names);
		
		@Override
		public VTLDataSetMetadata membership(String name);
		
		@Override
		public VTLDataSetMetadata subspace(Collection<? extends DataStructureComponent<Identifier, ? extends ValueDomainSubset<? extends ValueDomain>, ? extends ValueDomain>> subspace);
		
		@Override
		public VTLDataSetMetadata joinForOperators(DataStructure other);
		
		@Override
		public VTLDataSetMetadata rename(DataStructureComponent<?, ?, ?> component, String newName);
		
		@Override
		public VTLDataSetMetadata drop(Collection<String> names);
		
		@Override
		public <S extends ValueDomainSubset<D>, D extends ValueDomain> VTLDataSetMetadata pivot(DataStructureComponent<Identifier, StringCodeList, StringDomain> identifier,
				DataStructureComponent<Measure, S, D> measure);
	}
	
	public default Stream<DataPoint> concatDataPoints(DataSet other)
	{
		return Stream.concat(stream(), other.stream());
	}

	@Override
	public VTLDataSetMetadata getMetadata();
	
	public default long size()
	{
		return stream().count();
	}

	public Stream<DataPoint> stream();

	public VTLDataSetMetadata getDataStructure();

	public Collection<? extends DataStructureComponent<?, ?, ?>> getComponents();
	
	public default <R extends ComponentRole> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
	{
		return getComponents().stream()
				.filter(c -> c.is(typeOfComponent))
				.map(c -> c.as(typeOfComponent))
				.collect(Collectors.toSet());
	}

	public default <R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> Set<DataStructureComponent<R, S, D>> getComponents(Class<R> role,
			S domain)
	{
		return getComponents().stream()
				.filter(c -> c.is(role))
				.filter(c -> domain.isAssignableFrom(c.getDomain()))
				.map(c -> c.as(domain))
				.map(c -> c.as(role))
				.collect(Collectors.toSet());
	}

	/**
	 * Creates a new dataset retaining the specified component along with all identifiers of this dataset
	 * @param component The component to retain.
	 * 
	 * @return The projected dataset
	 */
	public DataSet membership(String component);

	/**
	 * Obtains a component with given name
	 * 
	 * @param name The requested component's name.
	 * @return The requested component, or null if no one was found.
	 */
	public Optional<DataStructureComponent<?, ?, ?>> getComponent(String name);

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

	public Stream<DataPoint> getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues);

	public default boolean contains(DataPoint datapoint)
	{
		return getMatching(datapoint.getValues(Identifier.class)).findAny().isPresent();
	}

	public default boolean notContains(DataPoint datapoint)
	{
		return !getMatching(datapoint.getValues(Identifier.class)).findAny().isPresent();
	}

	public DataSet filteredMappedJoin(VTLDataSetMetadata metadata, DataSet rightDataset, BiPredicate<DataPoint,DataPoint> filter, BinaryOperator<DataPoint> mergeOp);

	public <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter,
			BiFunction<? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, ? super Stream<DataPoint>, T> groupMapper);

	public default <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			BiFunction<? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, ? super Stream<DataPoint>, T> groupMapper)
	{
		return streamByKeys(keys, emptyMap(), groupMapper);
	}

	public DataSet filter(Predicate<DataPoint> predicate);

	public DataSet mapKeepingKeys(VTLDataSetMetadata metadata, Function<? super DataPoint, ? extends Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>>> operator);
}
