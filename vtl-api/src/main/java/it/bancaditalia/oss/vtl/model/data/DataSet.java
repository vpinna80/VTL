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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.SerTriFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

/**
 * The base interface describing a dataset
 * 
 * @author Valentino Pinna
 *
 */
public interface DataSet extends VTLValue, Iterable<DataPoint>
{
	public static final SerBiPredicate<DataPoint, DataPoint> ALL = (a,  b) -> true;

	/**
	 * @return The {@link DataSetStructure structure} of this DataSet.
	 */
	@Override
	public DataSetStructure getMetadata();
	
	/**
	 * @return a {@link Stream} of this dataset's {@link DataPoint}s.
	 * The stream does not conform to a particular ordering.
	 */
	public Stream<DataPoint> stream();

	/**
	 * Obtains an {@link Iterator} over the {@link DataPoint}s in the this DataSet.
	 * 
	 * NOTE: invoking this method may cause the invoking thread to deadlock if
	 * files must be opened to retrieve data points. To avoid deadlocks, first
	 * obtain a {@link DataSet#stream()} through a try-with-resources, then 
	 * obtain the iterator from that.
	 * 
	 * @return an {@link Iterator} of this dataset's {@link DataPoint}s.
	 * The iterating order is undefined and may change on subsequent invocations.
	 */
	@Override
	public default Iterator<DataPoint> iterator()
	{
		return stream().iterator();
	}
	
	/**
	 * Obtains a {@link Spliterator} over the {@link DataPoint}s in the this DataSet.
	 * 
	 * NOTE: invoking this method may cause the invoking thread to deadlock if
	 * files must be opened to retrieve data points. To avoid deadlocks, first
	 * obtain a {@link DataSet#stream()} through a try-with-resources, then 
	 * obtain the spliterator from that.
	 * 
	 * @return an {@link Spliterator} of this dataset's {@link DataPoint}s.
	 * The spliterator characteristics may vary depending on this DataSet input source.
	 */
	@Override
	public default Spliterator<DataPoint> spliterator()
	{
		return stream().spliterator();
	}

    /**
     * Performs the given action for each {@link DataPoint} in this DataSet.
     * The processing order is not specified and may change over 
     * subsequent invocations.
     *
     * @param action The action to be performed for each element
     */
	public default void forEach(Consumer<? super DataPoint> action)
	{
		try (Stream<DataPoint> stream = stream())
		{
			stream.forEach(action);
		}
	}

	/**
	 * Creates a new dataset retaining the specified component along with all identifiers of this dataset
	 * @param alias The alias of the component to retain.
	 * @param lineageOp The lineage enricher of this datapoint.
	 * @return The projected dataset
	 */
	public DataSet membership(VTLAlias alias, SerUnaryOperator<Lineage> lineageOp);

	/**
	 * Finds a component with given name
	 * 
	 * @param alias The requested component's alias.
	 * @return an {@link Optional} eventually containing the requested {@link DataSetComponent} if one was found.
	 */
	public Optional<DataSetComponent<?, ?, ?>> getComponent(VTLAlias alias);

	/**
	 * Create a new DataSet by filtering this DataSet's {@link DataPoint}s matching the specified values for some identifiers.
	 * 
	 * @param keyValues A {@link Map} containing values for some of this DataSet {@link Identifier}s.
	 *      If the map is empty, the result is this {@code DataSet}.
	 * @return A new {@code DataSet} of matching {@link DataPoint}s, eventually empty.
	 */
	public default DataSet getMatching(Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return filter(dp -> keyValues.equals(dp.getValues(keyValues.keySet(), Identifier.class)), SerUnaryOperator.identity());	
	}

	/**
	 * Enrich the lineage of each datapoint in this DataSet using the provided operator 
	 * @param lineageEnricher the operator
	 * @return A new dataset where each datapoint lineage is enriched.
	 */
	public VTLValue enrichLineage(SerUnaryOperator<Lineage> lineageEnricher);

	/**
	 * Creates a new DataSet by retaining only the datapoints in this DataSet which match a given {@link Predicate}.
	 * 
	 * @param predicate The {@link Predicate} to be applied.
	 * @param enricher the lineage enricher
	 * @return A new filtered DataSet. 
	 */
	public DataSet filter(SerPredicate<DataPoint> predicate, SerUnaryOperator<Lineage> enricher);

	/**
	 * Creates a new DataSet that represents the subspace of this DataSet with given identifiers having specific values
	 * @param keyValues A Map that gives the value for each identifier to subspace.
	 * @param lineageOp an operator to enrich the datapoint lineage 
	 * @return A new DataSet that is a subspace of this DataSet.  
	 */
	public DataSet subspace(Map<? extends DataSetComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keyValues, SerUnaryOperator<Lineage> lineageOp);
	
	/**
	 * Creates a new DataSet by transforming each of this DataSet's {@link DataPoint} by a given {@link Function}.
	 * 
	 * @param metadata The {@link DataSetStructure structure} the new dataset must conform to.
	 * @param lineageOp TODO
	 * @param operator a {@link Function} that maps each of this DataSet's {@link DataPoint}s.
	 * @return The new transformed DataSet. 
	 */
	public DataSet mapKeepingKeys(DataSetStructure metadata, SerUnaryOperator<Lineage> lineageOp, SerFunction<? super DataPoint, ? extends Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator);

	/**
	 * Creates a new DataSet by transforming each of this DataSet's {@link DataPoint} by a given {@link Function}.
	 * 
	 * @param metadata The {@link DataSetStructure structure} the new dataset must conform to.
	 * @param lineageOp TODO
	 * @param operator a {@link Function} that maps each of this DataSet's {@link DataPoint}s.
	 * @return The new transformed DataSet. 
	 */
	public DataSet flatmapKeepingKeys(DataSetStructure metadata, SerUnaryOperator<Lineage> lineageOp, SerFunction<? super DataPoint, ? extends Stream<? extends Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>>> operator);

	/**
	 * Creates a new DataSet by joining each DataPoint of this DataSet with all data points of another DataSet by matching the common identifiers.
	 * 
	 * @param metadata The {@link DataSetStructure structure} the new DataSet must conform to.
	 * @param other another DataSet that will be indexed and joined to each DataPoint of this DataSet.
	 * @param merge a {@link BinaryOperator} that merges two selected joined DataPoints together into one.
	 * @param having if not null, data points are filtered according to values of this component
	 * @return The new DataSet.
	 */
	public DataSet filteredMappedJoin(DataSetStructure metadata, DataSet other, SerBinaryOperator<DataPoint> merge,
		DataSetComponent<?, ? extends BooleanDomainSubset<?>, ? extends BooleanDomain> having);

	/**
	 * Creates a new DataSet by joining each DataPoint of this DataSet with all data points of another DataSet by matching the common identifiers.
	 * 
	 * @param metadata The {@link DataSetStructure structure} the new DataSet must conform to.
	 * @param other another DataSet that will be indexed and joined to each DataPoint of this DataSet.
	 * @param combiner the operator used to combine the two datapoints
	 * @return The new DataSet.
	 */
	public default DataSet mappedJoin(DataSetStructure metadata, DataSet other, SerBinaryOperator<DataPoint> combiner)
	{
		return filteredMappedJoin(metadata, other, combiner, null);
	}

	/**
	 * Perform a reduction over a dataset, producing a result for each group defined common values of the specified identifiers.
	 * If no grouping identifiers are specified, the dataset is aggregated to a scalar.
	 * 
	 * @param <T> The type of elements fed into the aggregation
	 * @param <TT> The type of the result produced by the aggregation
	 * @param resultMetadata the metadata of the result value produced
	 * @param keys the identifiers on whose values datapoints should be grouped 
	 * @param groupCollector the aggregator that performs the reduction
	 * @param finisher a finisher that may manipulate the result given the group where it belongs
	 * 
	 * @return a new VTL value, either a scalar or a dataset, that is the result of the aggregation.
	 */
	public <T extends Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT> VTLValue aggregate(VTLValueMetadata resultMetadata, 
			Set<DataSetComponent<Identifier, ?, ?>> keys, SerCollector<DataPoint, ?, T> groupCollector,
			SerTriFunction<? super T, ? super List<Lineage>, ? super Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT> finisher);
	
	/**
	 * Creates a new DataSet by applying a window function over a component of this DataSet.
	 * Each application can produce one or more values, in which case they will be exploded into multiple datapoints.
	 * The result values of the window function are stored in the destination component of the resulting dataset.
	 * 
	 * @param <T> The type of elements fed into the window
	 * @param <TT> The type of the result produced by the window function
	 * @param lineageOp A lineage definition for each result datapoint
	 * @param sourceComp The source component
	 * @param destComp The destination component
	 * @param clause The window clause
	 * @param extractor Extractor that feeds a value into the window. If null, the value fed 
	 *        will be the value assumed by the source component in each datapoint
	 * @param collector The collector that will give all the datafoints originating from each datapoint in a given window
	 * @param finisher A final mapping from the accumulated result into a collection of values.
	 *        If null, the collector result will be taken as-is, and it must be a collection of scalar values.
	 * 
	 * @return A new dataset resulting from the application of the specified window function.
	 */
	public <T, TT> DataSet analytic(SerUnaryOperator<Lineage> lineageOp, DataSetComponent<?, ?, ?> sourceComp,
			DataSetComponent<?, ?, ?> destComp, WindowClause clause, SerFunction<DataPoint, T> extractor,
			SerCollector<T, ?, TT> collector, SerBiFunction<TT, T, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisher);

	/**
	 * Creates a new DataSet as the union of this and other datasets.
	 * The datasets must have the same structure, and duplicated datapoints are taken from the leftmost operand
	 * 
	 * @param others The datasets to perform the union with
	 * @param lineageOp The lineage operator
	 * @return The result of the union. 
	 */
	public default DataSet union(List<DataSet> others, SerUnaryOperator<Lineage> lineageOp)
	{
		return union(others, lineageOp, true);
	}

	/**
	 * Creates a new DataSet as the union of this and other datasets. All the datasets must have the same structure as this DataSet.
	 * If <code>check</code> is true, the duplicated datapoints are taken only from the leftmost operand.
	 * 
	 * @param others The datasets to perform the union with
	 * @param lineageOp The lineage operator
	 * @param check True if a check of uniqueness of datapoints must be performed.
	 * @return The result of the union. 
	 */
	public DataSet union(List<DataSet> others, SerUnaryOperator<Lineage> lineageOp, boolean check);

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
	 * Checks if a DataPoint is contained in this DataSet.
	 * 
	 * <b>NOTE</b>: The default implementation performs a linear search, potentially traversing this DataSet entirely.
	 * @param datapoint the datapoint to check
	 * @return true if the DataPoint is contained in this DataSet
	 */
	public default boolean contains(DataPoint datapoint)
	{
		return getMatching(datapoint.getValues(Identifier.class)).stream().findAny().isPresent();
	}

	/**
	 * Checks if a DataPoint is not contained in this DataSet.
	 * 
	 * <b>NOTE</b>: The default implementation performs a linear search, potentially traversing this DataSet entirely.
	 * @param datapoint the datapoint to check
	 * @return true if the DataPoint is contained in this DataSet
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
	
	@Override
	public default boolean isDataSet()
	{
		return true;
	}
}
