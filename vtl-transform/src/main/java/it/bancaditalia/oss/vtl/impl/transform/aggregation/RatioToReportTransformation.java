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
package it.bancaditalia.oss.vtl.impl.transform.aggregation;

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl.DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.impl.types.operators.AnalyticOperator.SUM;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class RatioToReportTransformation extends UnaryTransformation implements AnalyticTransformation
{
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(RatioToReportTransformation.class);

	private final List<VTLAlias> partitionBy;

	public RatioToReportTransformation(Transformation operand, List<VTLAlias> partitionBy)
	{
		super(operand);

		this.partitionBy = coalesce(partitionBy, emptyList());
	}

	@Override
	protected VTLValue evalOnScalar(TransformationScheme scheme, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalOnDataset(TransformationScheme scheme, DataSet dataset, VTLValueMetadata metadata)
	{
		Set<DataSetComponent<Identifier, ?, ?>> partitionIDs = dataset.getMetadata().matchIdComponents(partitionBy, "partition by");
		Set<DataSetComponent<Measure, ?, ?>> measures = dataset.getMetadata().getMeasures();
		WindowClause clause = new WindowClauseImpl(partitionIDs, null, DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING);
		
		for (DataSetComponent<Measure, ?, ?> measure: measures)
		{
			SerBiFunction<ScalarValue<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>, Collection<? extends ScalarValue<?, ?, ?, ?>>> divider = (sumValue, measureValue) -> {
				if (sumValue == null || sumValue.isNull() || measureValue == null || measureValue.isNull())
					return singleton(NullValue.instanceFrom(measure));
				else if (isUseBigDecimal())
				{
					BigDecimal old = (BigDecimal) createNumberValue((Number) measureValue.get()).get();
					BigDecimal sum = (BigDecimal) createNumberValue((Number) sumValue.get()).get();
					return singleton(createNumberValue(old.divide(sum)));
				}
				else
				{
					double old = ((Number) measureValue.get()).doubleValue();
					double sum = ((Number) sumValue.get()).doubleValue();
					return singleton(createNumberValue(old / sum));
				}
			};

			SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> reducer = SUM.getReducer(measure.getDomain());
			dataset = dataset.analytic(lineageEnricher(this), measure, measure, clause, null, reducer, divider);
		}
		
		return dataset;
	}

	@Override
	public DataSetStructure computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session);
		
		if (!opmeta.isDataSet())
			throw new VTLInvalidParameterException(opmeta, DataSetStructure.class);
		
		DataSetStructure dataset = (DataSetStructure) opmeta;
		
		dataset.matchIdComponents(partitionBy, "partition by");
		
		return new DataSetStructureBuilder(dataset.getIDs())
				.addComponents(dataset.getMeasures())
				.build();
	}
	
	@Override
	public String toString()
	{
		return "ratio_to_report(" + operand + " over (" 
				+ (partitionBy != null ? partitionBy.stream().map(VTLAlias::toString).collect(joining(", ", " partition by ", " ")) : "")
				+ "))";
	}
}
