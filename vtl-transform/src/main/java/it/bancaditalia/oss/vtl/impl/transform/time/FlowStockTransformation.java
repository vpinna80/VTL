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
package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.transform.time.FlowStockTransformation.DatasetOperator.STOCK_TO_FLOW;
import static it.bancaditalia.oss.vtl.impl.transform.util.LimitClause.CURRENT_DATA_POINT;
import static it.bancaditalia.oss.vtl.impl.transform.util.LimitClause.preceding;
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl.DATAPOINTS_UNBOUNDED_PRECEDING_TO_CURRENT;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.DATAPOINTS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.filtering;
import static it.bancaditalia.oss.vtl.util.SerCollectors.reducing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.util.SortClause;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDurationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;

public class FlowStockTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public enum DatasetOperator implements SerBinaryOperator<Number>, Serializable
	{
		STOCK_TO_FLOW("stock_to_flow", (a, b) -> b - a, (a, b) -> b - a, (a, b) -> b.subtract(a)), 
		FLOW_TO_STOCK("flow_to_stock", Long::sum, Double::sum, BigDecimal::add); 
		
		private final String text;
		private final SerBinaryOperator<Long> longFun;
		private final SerBinaryOperator<Double> dblFun;
		private final SerBinaryOperator<BigDecimal> bigFun;

		private DatasetOperator(String text, SerBinaryOperator<Long> longFun, SerBinaryOperator<Double> dblFun, SerBinaryOperator<BigDecimal> bigFun)
		{
			this.text = text;
			this.longFun = longFun;
			this.dblFun = dblFun;
			this.bigFun = bigFun;
		}

		@Override
		public String toString()
		{
			return text;
		}
		
		public Number apply(Number a, Number b)
		{
			if (a instanceof Long && b instanceof Long)
				return longFun.apply(a.longValue(), b.longValue());
			else if (isUseBigDecimal())
			{
				BigDecimal da = a instanceof BigDecimal ? (BigDecimal) a : a instanceof Double ? new BigDecimal((double) a) : new BigDecimal((long) a);
				BigDecimal db = b instanceof BigDecimal ? (BigDecimal) b : b instanceof Double ? new BigDecimal((double) b) : new BigDecimal((long) b);
				return bigFun.apply(da, db);
			}
			else
				return dblFun.apply(a.doubleValue(), b.doubleValue());
		}
	}

	private final DatasetOperator operator;

	public FlowStockTransformation(DatasetOperator operator, Transformation operand)
	{
		super(operand);
		this.operator = operator;
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		DataSetMetadata dsMeta = dataset.getMetadata();
		DataStructureComponent<Measure, EntireDurationDomainSubset, DurationDomain> freq = DURATIONDS.getDefaultVariable().as(Measure.class);

		Set<DataStructureComponent<Measure, ?, ?>> measures = dsMeta.getMeasures();
		DataStructureComponent<Identifier, ?, ?> timeId = dsMeta.getSingleton(Identifier.class, TIMEDS); 
		Set<DataStructureComponent<?, ?, ?>> ids = new HashSet<>(dsMeta.getIDs());
		ids.remove(timeId);
		ids.add(freq);
		
		DataSetMetadata metaWithFreq = new DataStructureBuilder(dsMeta).addComponent(freq).build(); 
		dataset = dataset.mapKeepingKeys(metaWithFreq, DataPoint::getLineage, dp -> new DataPointBuilder(dp)
				.add(freq, ((TimeValue<?, ?, ?, ?>) dp.getValue(timeId)).getFrequency())
				.build(dp.getLineage(), metaWithFreq));

		DataSet partial = dataset;
		// compute stock or flow one measure at a time
		for (DataStructureComponent<Measure, ?, ?> measure: measures)
		{
			SerFunction<DataPoint, Lineage> lineageOp = lineageEnricher(this);
			WindowCriterion size = operator == STOCK_TO_FLOW ? new WindowCriterionImpl(DATAPOINTS, preceding(1), CURRENT_DATA_POINT) : DATAPOINTS_UNBOUNDED_PRECEDING_TO_CURRENT;
			WindowClause window = new WindowClauseImpl(ids, List.of(new SortClause(timeId)), size);
			SerFunction<DataPoint, Number> extractor = dp -> (Number) dp.get(measure).get();

			Class<?> repr;
			if (Domains.INTEGERDS.isAssignableFrom(measure.getVariable().getDomain())) 
				repr = Long.class;
			else if (isUseBigDecimal())
				repr = BigDecimal.class;
			else
				repr = Double.class;

			SerFunction<Optional<Number>, ScalarValue<?, ?, ?, ?>> create = repr == Long.class
					? IntegerValue::of
					: NumberValueImpl::createNumberValue;
			
			SerCollector<Number, ?, ScalarValue<?, ?, ?, ?>> collector = collectingAndThen(collectingAndThen(
					filtering(Objects::nonNull, reducing(repr, operator::apply)), create), measure.getVariable().getDomain()::cast);
			
			partial = partial.analytic(lineageOp, measure, measure, window, extractor, collector, null);
		}
		
		// remove the freq measure
		return partial.mapKeepingKeys((DataSetMetadata) metadata, DataPoint::getLineage, dp -> {
				var map = new HashMap<>(dp.getValues(NonIdentifier.class));
				map.remove(freq);
				return map;
			});
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
			
			Set<? extends DataStructureComponent<Identifier, ?, ?>> ids = dsmeta.getIDs().stream()
					.filter(c -> c.getVariable().getDomain() instanceof TimeDomainSubset)
					.collect(toSet());
			
			if (ids.size() == 0)
				throw new VTLMissingComponentsException(VTLAliasImpl.of("Time identifier"), ids);
			
			Set<? extends DataStructureComponent<Measure, ?, ?>> measures = dsmeta.getMeasures();
			if (measures.size() == 0)
				throw new VTLMissingComponentsException(VTLAliasImpl.of("At least one numeric measure"), dsmeta);
			
			for (DataStructureComponent<Measure, ?, ?> measure: measures)
				if (!NUMBERDS.isAssignableFrom(measure.getVariable().getDomain()))
					throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, measure);

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
