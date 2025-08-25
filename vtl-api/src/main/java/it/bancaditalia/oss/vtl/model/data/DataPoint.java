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
import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.entryByKeyValue;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollectors;
import it.bancaditalia.oss.vtl.util.SerComparator;
import it.bancaditalia.oss.vtl.util.SerToIntBiFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

/**
 * An immutable representation of a datapoint of a VTL dataset.
 * 
 * @author Valentino Pinna
 */
public interface DataPoint extends Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, Serializable
{
	/**
	 * Defines a {@link Comparator} that enforces an ordering using the values of given identifiers
	 * 
	 * @param components the component whose values are used for the ordering
	 * @return the Comparator instance
	 */
	public static SerComparator<DataPoint> compareBy(List<DataSetComponent<?, ?, ?>> components)
	{
		SerToIntBiFunction<DataPoint, DataPoint> comparator = null;
		for (DataSetComponent<?, ?, ?> component: components)
		{
			if (comparator == null)
				comparator = (dp1, dp2) -> {
					return dp1.get(component).compareTo(dp2.get(component));
				};
			else
			{
				SerToIntBiFunction<DataPoint, DataPoint> prevComparator = comparator;
				comparator = (dp1, dp2) -> {
					int r = prevComparator.applyAsInt(dp1, dp2);
					if (r == 0)
						return dp1.get(component).compareTo(dp2.get(component));
					else
						return r;
				};
			}
		};
		return comparator::applyAsInt;
	}

	/**
	 * Creates a new datapoint dropping all provided non-id components
	 * 
	 * @param components the components to drop
	 * @return a new datapoint without the provided components.
	 */
	public DataPoint drop(Collection<? extends DataSetComponent<? extends NonIdentifier, ?, ?>> components);

	/**
	 * Creates a new datapoint keeping all the identifiers and only the provided non-id components
	 * 
	 * @param components the components to drop
	 * @return a new datapoint with all existing ids and the provided components.
	 */
	public DataPoint keep(Collection<? extends DataSetComponent<? extends NonIdentifier, ?, ?>> components);

	/**
	 * Creates a new datapoint renaming the provided component to another one with the same role.
	 *
	 * @param oldComponent the component to be renamed
	 * @param newComponent the already renamed component
	 * @return a new datapoint with the old component renamed.
	 */
	public DataPoint rename(DataSetComponent<?, ?, ?> oldComponent, DataSetComponent<?, ?, ?> newComponent);

	/**
	 * Create a new datapoint combining this and another datapoint.
	 * All existing components keep their values in this datapoint and aren't updated with new values.
	 * @param other the datapoint to combine with this datapoint
	 * @param lineageCombiner An operator used to combine the lineages of the two data points
	 * 
	 * @return a new datapoint that is the combination of this and another datapoint.
	 */
	public DataPoint combine(DataPoint other, SerBinaryOperator<Lineage> lineageCombiner);

	/**
	 * Get the source transformation of this DataPoint
	 * 
	 * @return the transformation from where the datapoint originated 
	 */
	public Lineage getLineage();


	/**
	 * Create a new DataPoint by enriching its lineage information with the provided function.
	 * @param enricher The lineage enricher
	 * @return the new enriched datapoint 
	 */
	public DataPoint enrichLineage(SerUnaryOperator<Lineage> enricher);
	
	/**
	 * If the component exists, retrieves the value for it in this datapoint, performing a cast of the result.
	 * 
	 * @param <S> the domain subset type of the component
	 * @param <D> the domain type of the component
	 * @param component the component to query
	 * @return the casted value for the component if exists.
	 * @throws NullPointerException if the component is not found
	 */
	public default <S extends ValueDomainSubset<S, D>, D extends ValueDomain> ScalarValue<?, ?, S, D> getValue(DataSetComponent<?, S, D> component)
	{
		return component.getDomain().cast(get(component));
	}
	
	/**
	 * Checks if this datapoint identifiers' values match all the provided ones.
	 * 
	 * @param identifierValues the id values to check
	 * @return true if this datapoint matches the provided identifiers' values.
	 */
	public default boolean matches(Map<? extends DataSetComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> identifierValues)
	{
		return !Utils.getStream(identifierValues)
				.filter(entryByKeyValue((k, v) -> !get(k).equals(k.getDomain().cast(v))))
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
	public <R extends Component> Map<DataSetComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role);

	/**
	 * Returns the values for multiple components.
	 * 
	 * @param components the collection of components to query
	 * @return a map with the values for the specified components.
	 */
	public default Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Collection<? extends DataSetComponent<?, ?, ?>> components)
	{
		return Utils.getStream(components)
				.filter(this::containsKey)
				.collect(toMapWithValues(this::get));
	}

	/**
	 * Returns the values for multiple components having a specified role and matching one of the specified names.
	 * 
	 * @param <R> the component role type
	 * @param role role of the components
	 * @param names collection of names
	 * @return a map with the values for all the components having the specified role and matching one of the specified names.
	 */
	public default <R extends Component> Map<DataSetComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role, Collection<VTLAlias> names)
	{
		return Utils.getStream(keySet())
				.map(c -> new SimpleEntry<>(c, c.getAlias()))
				.filter(entryByValue(names::contains))
				.map(Entry::getKey)
				.filter(c -> c.is(role))
				.map(c -> c.asRole(role))
				.collect(toMapWithValues(this::get));
	}

	/**
	 * Returns the values for the chosen components having the specified names.
	 * 
	 * @param names The names of the component
	 * @return map with the values for the chosen components
	 */
	public default Map<DataSetComponent<?, ?, ?>, ScalarValue<?,?,?,?>> getValuesByNames(Collection<VTLAlias> names)
	{
		return Utils.getStream(keySet())
				.map(c -> new SimpleEntry<>(c.getAlias(), c))
				.filter(entryByKey(names::contains))
				.collect(Collectors.toMap(Entry::getValue, e -> get(e.getValue())));

	}
	
	/**
	 * Returns the values for the chosen components having a specified role.
	 * 
	 * @param <R> the component role type
	 * @param role role of the components
	 * @param components collection of components to query
	 * @return a map with the values for the chosen components having the specified role.
	 */
	public default <R extends Component> Map<DataSetComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Collection<? extends DataSetComponent<R, ?, ?>> components, Class<R> role)
	{
		return Utils.getStream(getValues(role).entrySet())
				.filter(entryByKey(components::contains))
				.map(keepingValue(c -> c.asRole(role)))
				.collect(SerCollectors.entriesToMap());
	}
	
	/**
	 * Returns a distance, in term of components and values, between this and another DataPoint.
	 * The distance is computed as the number of components present only in one datapoint,
	 * plus the number of all the components present in both datapoints which have a different value respectively.
	 *  
	 * @param other The other datapoint.
	 * @return A positive integer representing the distance, 0 if equals. 
	 */
	public default int getDistance(DataPoint other)
	{
		Set<DataSetComponent<?, ?, ?>> cmp1 = this.keySet();
		Set<DataSetComponent<?, ?, ?>> cmp2 = other.keySet();
		Set<DataSetComponent<?, ?, ?>> intersect = new HashSet<>(cmp1);
		intersect.retainAll(cmp2);
		
		int distance = cmp1.size() + cmp2.size() - intersect.size() * 2;
		for (DataSetComponent<?, ?, ?> c: intersect)
			if (!this.get(c).equals(other.get(c)))
				distance++;
		
		return distance;
	}
}
