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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.RANGE;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentSet;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.SerSupplier;
import it.bancaditalia.oss.vtl.util.SerTriFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public abstract class AbstractDataSet implements DataSet
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataSet.class);

	protected final DataSetStructure dataStructure;

	protected AbstractDataSet(DataSetStructure dataStructure)
	{
		this.dataStructure = dataStructure;
	}

	private static AbstractDataSet ofLambda(DataSetStructure metadata, SerSupplier<Stream<DataPoint>> supplier)
	{
		return new AbstractDataSet(metadata) {
			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return supplier.get();
			}
		};
	}

	@Override
	public DataSet membership(VTLAlias alias, SerUnaryOperator<Lineage> lineageOperator)
	{
		DataSetStructure membershipStructure = dataStructure.membership(alias);
		LOGGER.debug("Creating dataset by membership on {} from {} to {}", alias, dataStructure, membershipStructure);
		
		DataSetComponent<?, ?, ?> sourceComponent = dataStructure.getComponent(alias)
				.orElseThrow((Supplier<? extends RuntimeException> & Serializable) () -> new VTLMissingComponentsException(dataStructure, alias));
		DataSetComponent<? extends NonIdentifier, ?, ?> membershipMeasure = membershipStructure.getMeasures().iterator().next();

		SerFunction<DataPoint, Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> operator = dp -> {
				Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(); 
				map.put(membershipMeasure, dp.get(sourceComponent));
				map.putAll(dp.getValues(membershipStructure.getComponents(ViralAttribute.class)));
				return map;
			};
		
		return mapKeepingKeys(membershipStructure, lineageOperator, operator);
	}
	
	@Override
	public DataSet subspace(Map<? extends DataSetComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keyValues, SerUnaryOperator<Lineage> lineageOperator)
	{
		DataSetStructure newMetadata = new DataSetStructureBuilder(dataStructure).removeComponents(keyValues.keySet()).build();
		
		return ofLambda(newMetadata, () -> stream()
			.filter(dp -> dp.matches(keyValues))
			.map(dp -> new DataPointBuilder(dp)
					.delete(keyValues.keySet())
					.build(lineageOperator.apply(dp.getLineage()), newMetadata)));
	}

	@Override
	public Optional<DataSetComponent<?, ?, ?>> getComponent(VTLAlias name)
	{
		return dataStructure.getComponent(name);
	}

	@Override
	public DataSetStructure getMetadata()
	{
		return dataStructure;
	}
	
	@Override
	public DataSet filteredMappedJoin(DataSetStructure metadata, DataSet other,
		SerBinaryOperator<DataPoint> mergeOp,
		DataSetComponent<?, ? extends BooleanDomainSubset<?>, ? extends BooleanDomain> having)
	{
		Set<DataSetComponent<Identifier, ?, ?>> ids = getMetadata().getIDs();
		Set<DataSetComponent<Identifier, ?, ?>> otherIds = other.getMetadata().getIDs();
		Set<DataSetComponent<Identifier, ?, ?>> commonIds = new HashSet<>(ids);
		commonIds.retainAll(otherIds);
		
		Map<Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Set<DataPoint>> index;
		try (Stream<DataPoint> stream = other.stream())
		{
			if (otherIds.equals(ids))
				// more performance if joining over all keys
				index = stream.collect(toConcurrentMap(dp -> dp.getValues(commonIds, Identifier.class), Collections::singleton));
			else
				index = stream.collect(groupingByConcurrent(dp -> dp.getValues(commonIds, Identifier.class), toConcurrentSet()));
		}
		
		return ofLambda(metadata, () -> stream()
				.map(dpThis -> flatMapDataPoint(having, mergeOp, dpThis, index.get(dpThis.getValues(commonIds, Identifier.class))))
				.collect(concatenating(ORDERED)));
	}

	protected static Stream<DataPoint> flatMapDataPoint(DataSetComponent<?, ? extends BooleanDomainSubset<?>, ? extends BooleanDomain> having,
			BinaryOperator<DataPoint> mergeOp, DataPoint dpThis, Collection<DataPoint>  otherSubGroup)
	{
		if (otherSubGroup == null)
			return Stream.empty();
		else if (having == null)
			return Utils.getStream(otherSubGroup).map(dpOther -> mergeOp.apply(dpThis, dpOther));
		else if (dpThis.containsKey(having))
			if (dpThis.get(having) == BooleanValue.TRUE)
				return Utils.getStream(otherSubGroup).map(dpOther -> mergeOp.apply(dpThis, dpOther));
			else
				return Stream.empty();
		else
			return Utils.getStream(otherSubGroup).filter(dpOther -> dpOther.get(having) == BooleanValue.TRUE)
				.map(dpOther -> mergeOp.apply(dpThis, dpOther));
	}

	@Override
	public DataSet mapKeepingKeys(DataSetStructure metadata, SerUnaryOperator<Lineage> lineageOperator, 
			SerFunction<? super DataPoint, ? extends Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		final Set<DataSetComponent<Identifier, ?, ?>> identifiers = dataStructure.getIDs();
		if (!metadata.getIDs().containsAll(identifiers))
			throw new VTLInvariantIdentifiersException("map", identifiers, metadata.getIDs());
		
		LOGGER.debug("Creating dataset by mapping from {} to {}", dataStructure, metadata);
		
		return new AbstractDataSet(metadata) {
			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.stream().map(this::mapper);
			}
			
			private DataPoint mapper(DataPoint dp)
			{
				return new DataPointBuilder(dp.getValues(Identifier.class), DONT_SYNC)
							.addAll(operator.apply(dp))
							.build(lineageOperator.apply(dp.getLineage()), dataStructure);
			}
		};
	}

	@Override
	public DataSet flatmapKeepingKeys(DataSetStructure metadata, SerUnaryOperator<Lineage> lineageOp,
			SerFunction<? super DataPoint, ? extends Stream<? extends Map<? extends DataSetComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>>> operator)
	{
		final Set<DataSetComponent<Identifier, ?, ?>> identifiers = dataStructure.getIDs();
		if (!metadata.getIDs().containsAll(identifiers))
			throw new VTLInvariantIdentifiersException("map", identifiers, metadata.getIDs());
		
		LOGGER.debug("Creating dataset by mapping from {} to {}", dataStructure, metadata);
		
		// TODO: check why map/reduce doesn't work and flatmap is mandatory
		return ofLambda(metadata, () -> stream()
			.flatMap(dp -> {
				Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> nonIDValues = dp.getValues(Identifier.class);
				return operator.apply(dp)
					.map(map -> new DataPointBuilder(nonIDValues).addAll(map).build(lineageOp.apply(dp.getLineage()), metadata));
			}));
	}

	@Override
	public <T extends Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT> VTLValue aggregate(VTLValueMetadata metadata, 
			Set<DataSetComponent<Identifier, ?, ?>> keys, SerCollector<DataPoint, ?, T> groupCollector,
			SerTriFunction<? super T, ? super List<Lineage>, ? super Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT> finisher)
	{
		// if the result is a dataset, then we are performing a group by
		if (metadata.isDataSet())
			return new StreamWrapperDataSet((DataSetStructure) metadata, () -> {
				try (Stream<DataPoint> stream = AbstractDataSet.this.stream())
				{
					Map<Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Entry<T, List<Lineage>>> result = stream.collect(
							groupingByConcurrent(dp -> dp.getValues(keys, Identifier.class), teeing(groupCollector, mapping(DataPoint::getLineage, toList()), SimpleEntry::new)));
					
					return Utils.getStream(result)
						.map(splitting((k, e) -> (DataPoint) finisher.apply(e.getKey(), e.getValue(), k)));
				}
			});
		// Otherwise the result is a scalar
		else
			try (Stream<DataPoint> stream = stream())
			{
				return (ScalarValue<?, ?, ?, ?>) finisher.apply(stream.collect(groupCollector), null, null);
			}
	}

	@Override
	public <T, TT> DataSet analytic(SerUnaryOperator<Lineage> lineageOp, DataSetComponent<?, ?, ?> sourceComp, DataSetComponent<?, ?, ?> destComp, WindowClause clause,
			SerFunction<DataPoint, T> extractor, SerCollector<T, ?, TT> collector, SerBiFunction<TT, T, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisher)
	{
		if (clause.getWindowCriterion() != null && clause.getWindowCriterion().getType() == RANGE)
			throw new UnsupportedOperationException("Range windows are not implemented in analytic invocation");

		DataSetStructure newStructure = new DataSetStructureBuilder(getMetadata())
				.addComponent(destComp)
				.build();
		
		return new AnalyticDataSet<>(this, newStructure, lineageOp, clause, sourceComp, destComp, extractor, collector, finisher);
	}
	
	@Override
	public DataSet union(List<DataSet> others, SerUnaryOperator<Lineage> lineageOp, boolean check)
	{
		// Fast track when the functional aspect is preserved
		if (!check)
			return ofLambda(dataStructure, () -> Stream.concat(Stream.of(AbstractDataSet.this), others.stream())
					.map(DataSet::stream)
					.collect(concatenating(ORDERED)));
		
		Set<DataSetComponent<Identifier, ?, ?>> ids = dataStructure.getIDs();
		for (DataSet other: others)
			if (!dataStructure.equals(other.getMetadata()))
				throw new InvalidParameterException("Union between two datasets with different structures: " + dataStructure + " and " + other.getMetadata()); 

		List<Set<DataPoint>> results = new ArrayList<>();
		Set<Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> seen = newKeySet();
		LOGGER.info("Evaluating first union operand");
		try (Stream<DataPoint> stream = stream())
		{
			results.add(stream
					.peek(dp -> seen.add(dp.getValues(ids)))
					.map(dp -> dp.enrichLineage(lineageOp))
					.collect(toConcurrentSet())
			);
		}
		
		// eagerly compute the differences (one set at a time to avoid OutOfMemory and preserve "leftmost rule")
		int nOperand = 2;
		for (DataSet other: others)
			try (Stream<DataPoint> stream = other.stream())
			{
				LOGGER.info("Evaluating union operand {}", nOperand++);
				Stream<DataPoint> stream2 = stream;
				if (LOGGER.isTraceEnabled())
					stream2 = stream2.peek(dp -> {
						if (seen.contains(dp.getValues(ids)))
							LOGGER.trace("Union: Found a duplicated datapoint in {}: {}", other, dp);
					});

				results.add(stream2
						.filter(dp -> seen.add(dp.getValues(ids)))
						.map(dp -> dp.enrichLineage(lineageOp))
						.collect(toConcurrentSet()));
			}
		
		// concat all datapoints from all sets
		return new FunctionDataSet<>(dataStructure, list -> Utils.getStream(results)
				.map(Utils::getStream)
				.collect(concatenating(ORDERED)), results);
	}

	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate, SerUnaryOperator<Lineage> linOp)
	{
		return new StreamWrapperDataSet(dataStructure, () -> stream()
			.filter(predicate)
			.map(dp -> new DataPointBuilder(dp).build(linOp.apply(dp.getLineage()), dataStructure)));
	}
	
	@Override
	public VTLValue enrichLineage(SerUnaryOperator<Lineage> lineageEnricher)
	{
		return new AbstractDataSet(dataStructure) {
			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return AbstractDataSet.this.stream().map(dp -> dp.enrichLineage(lineageEnricher));
			}
		};
	}
	
	@Override
	public String toString()
	{
		return "#UNNAMED#" + getMetadata();
	}
	
	@Override
	public final Stream<DataPoint> stream()
	{
		LOGGER.debug("Requested streaming of {}", this);
		
		Stream<DataPoint> stream = streamDataPoints();
		if (LOGGER.isTraceEnabled())
		{
			AtomicBoolean dontpeek = new AtomicBoolean(false);
			Map<Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> seen = new ConcurrentHashMap<>() {
				private static final long serialVersionUID = 1L;
				
				private final String id = UUID.randomUUID().toString();
				
				public DataPoint put(Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> key, DataPoint value)
				{
					LOGGER.error("UUID: {}; Hash: {} dp: {}", id, Objects.hashCode(value), value);
					return super.put(key, value);
				};
			};
			stream = stream.peek(dp -> checkDuplicates(dontpeek, seen, dp))
				.onClose(() -> LOGGER.trace("Closing stream for {}", this))
				.onClose(() -> seen.clear());
		}
		
		return stream;
	}

	private void checkDuplicates(AtomicBoolean dontpeek, Map<Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> seen, DataPoint dp)
	{
		if (!dontpeek.get())
		{
			if (this instanceof NamedDataSet)
				LOGGER.trace("Dataset {} output datapoint {}", ((NamedDataSet) this).getAlias(),  dp);
			else
				LOGGER.trace("Dataset {} output datapoint {}", dp.getLineage(), dp);
			Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyVals = dp.getValues(Identifier.class);
			DataPoint prev = seen.put(keyVals, dp);
			if (prev != null)
			{
				LOGGER.error("Duplicated datapoints with key {}:\n    1)  {}\n    2)  {}", keyVals, dp, prev);
				throw new IllegalStateException("Duplicated datapoints with hashcodes: " + dp.hashCode() + " - " + prev.hashCode());
			}
		}
		else
		{
			dontpeek.set(true);
			seen.clear();
		}
	}
	
	protected abstract Stream<DataPoint> streamDataPoints();
}
