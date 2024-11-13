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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.DIFF;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.SUM;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static java.util.Collections.emptyMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.BiFunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDurationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.Utils;

public class FlowStockTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(FlowStockTransformation.class);

	private final static boolean bothIntegers(ScalarValue<?, ?, ?, ?> l, ScalarValue<?, ?, ?, ?> r)
	{
		return l instanceof IntegerValue && r instanceof IntegerValue;
	}

	public enum DatasetOperator implements SerBiFunction<DataSet, DataStructureComponent<Identifier, ?, ?>, Stream<DataPoint>>
	{
		STOCK_TO_FLOW("stock_to_flow", false, (b, a) -> bothIntegers(b, a) ? DIFF.applyAsInteger(a, b) : DIFF.applyAsNumber(a, b)), 
		FLOW_TO_STOCK("flow_to_stock", true, (acc, v) -> bothIntegers(acc, v) ? SUM.applyAsInteger(acc, v) : SUM.applyAsNumber(acc, v)); 
		
		private final BinaryOperator<ScalarValue<?, ?, ?, ?>> op;
		private final String text;
		private final boolean cumulating;

		private DatasetOperator(String text, boolean cumulating, BinaryOperator<ScalarValue<?, ?, ?, ?>> op)
		{
			this.text = text;
			this.cumulating = cumulating;
			this.op = op; 
		}

		@Override
		public Stream<DataPoint> apply(DataSet ds, DataStructureComponent<Identifier, ?, ?> timeid)
		{
			DataSetMetadata metadata = ds.getMetadata();
			DataStructureComponent<Identifier, EntireDurationDomainSubset, DurationDomain> freq = DURATIONDS.getDefaultVariable().as(Identifier.class);
			Set<DataStructureComponent<Measure, ?, ?>> measures = new HashSet<>(ds.getMetadata().getMeasures());
			Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(ds.getMetadata().getIDs());
			ids.remove(timeid);
			ids.add(freq);
			
			DataSetMetadata metaWithFreq = new DataStructureBuilder(metadata).addComponent(freq).build(); 
			ds = ds.mapKeepingKeys(metaWithFreq, DataPoint::getLineage, dp -> new DataPointBuilder(dp)
					.add(freq, ((TimeValue<?, ?, ?, ?>) dp.getValue(timeid)).getFrequency())
					.build(dp.getLineage(), metaWithFreq));

			return ds.streamByKeys(ids, emptyMap(), toConcurrentMap(i -> i, i -> true, (a, b) -> a, () -> new ConcurrentSkipListMap<>(DataPoint.compareBy(timeid))), (a, b) -> a)
				.map(Map::keySet)
				.map(group -> {
					Map<DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> acc = new ConcurrentHashMap<>();
					return group.stream().map(dp -> new DataPointBuilder(measures.stream()
							.collect(toMapWithValues(m -> {
								ScalarValue<?, ?, ?, ?> v = acc.merge(m, dp.get(m), op);
								if (!cumulating)
									acc.put(m, dp.get(m));
								return v; 
							}))).addAll(dp.getValues(Identifier.class))
							.delete(freq)
							.build(LineageNode.of(toString().toLowerCase(), dp.getLineage()), metadata));
				}).collect(concatenating(Utils.ORDERED));
		}
		
		@Override
		public String toString()
		{
			return text;
		}
	}

	private final DatasetOperator operator;
	private transient DataStructureComponent<Identifier, ?, ?> main;

	public FlowStockTransformation(DatasetOperator operator, Transformation operand)
	{
		super(operand);
		this.operator = operator;
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		return new BiFunctionDataSet<>((DataSetMetadata) metadata, (ds, timeId) -> operator.apply(ds, timeId), dataset, main);
	}
	
	@Override
	protected final VTLValue evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		throw new VTLInvalidParameterException(scalar, DataSet.class); 
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata metadata = operand.getMetadata(session);
		
		if (metadata instanceof DataSetMetadata)
		{
			DataSetMetadata dsmeta = (DataSetMetadata) metadata;

			if (main != null)
				return dsmeta;
			
			Set<? extends DataStructureComponent<Identifier, ?, ?>> ids = dsmeta.getIDs().stream()
					.filter(c -> c.getVariable().getDomain() instanceof TimeDomainSubset)
					.collect(toSet());
			
			if (ids.size() == 0)
				throw new VTLMissingComponentsException(VTLAliasImpl.of("Time identifier"), ids);
			
			main = dsmeta.contains(VTLAliasImpl.of("TIME_PERIOD")) ? dsmeta.getComponent(VTLAliasImpl.of("TIME_PERIOD"), Identifier.class).get() : ids.iterator().next(); 
			if (ids.size() > 1)
			{
				LOGGER.warn("Expected only one identifier, but found: " + ids);
				LOGGER.warn("Results may vary between executions!!!");
				LOGGER.warn(main + " will be chosen as date/time identifier.");
			}
			
			Set<? extends DataStructureComponent<Measure, ?, ?>> measures = dsmeta.getMeasures();
			if (measures.size() == 0)
				throw new VTLMissingComponentsException(VTLAliasImpl.of("At least one numeric measure"), dsmeta);
			
			for (DataStructureComponent<Measure, ?, ?> measure: measures)
				if (!NUMBERDS.isAssignableFrom(measure.getVariable().getDomain()))
					throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, measure.getVariable().getDomain());

			return dsmeta;
		}
		else
			throw new VTLInvalidParameterException(metadata, DataSetMetadata.class);
	}
	
	@Override
	public String toString()
	{
		return operator + "(" + operand + ")";
	}
}
