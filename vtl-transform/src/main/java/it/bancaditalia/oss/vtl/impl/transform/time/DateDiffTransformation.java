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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.INT_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.time.temporal.ChronoUnit.DAYS;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;

public class DateDiffTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;

	public DateDiffTransformation(Transformation left, Transformation right)
	{
		super(left, right);
	}

	@Override
	protected ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		return computeScalar(left, right);
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		DataSetComponent<Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		
		SerBinaryOperator<ScalarValue<?, ?, ?, ?>> func = datasetIsLeftOp ? DateDiffTransformation::computeScalar : (a, b) -> computeScalar(b, a);
		
		return dataset.mapKeepingKeys((DataSetStructure) metadata, lineageEnricher(this), dp -> {
			Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(NonIdentifier.class));
			map.remove(measure);
			map.put(INT_VAR, func.apply(dp.get(measure), scalar));
			return map;
		});
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		Set<DataSetComponent<Identifier, ?, ?>> idsLeft = left.getMetadata().getIDs();
		Set<DataSetComponent<Identifier, ?, ?>> idsRight = right.getMetadata().getIDs();
		DataSetComponent<Measure, ?, ?> measure = left.getMetadata().getMeasures().iterator().next();
		
		DataSet streamed = idsLeft.size() >= idsRight.size() ? left : right;
		DataSet indexed = streamed == left ? right : left;
		
		SerBinaryOperator<ScalarValue<?, ?, ?, ?>> func = streamed == left ? DateDiffTransformation::computeScalar : (a, b) -> computeScalar(b, a);
		
		SerBinaryOperator<Lineage> enricher = LineageNode.lineage2Enricher(this);
		return streamed.mappedJoin((DataSetStructure) metadata, indexed, (dps, dpi) -> new DataPointBuilder(dps.getValues(Identifier.class), DONT_SYNC)
			.addAll(dpi.getValues(Identifier.class))
			.delete(measure)
			.add(INT_VAR, func.apply(dps.get(measure), dps.get(measure)))
			.build(enricher.apply(dps.getLineage(), dpi.getLineage()), (DataSetStructure) metadata));
	}

	private static ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> computeScalar(ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left.isNull() || right.isNull())
			return NullValue.instance(INTEGERDS);
		
		LocalDate dLeft = ((TimeValue<?, ?, ?, ?>) left).getEndDate().get();
		LocalDate dRight = ((TimeValue<?, ?, ?, ?>) right).getEndDate().get();
		
		return IntegerValue.of(dLeft.until(dRight, DAYS));
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		if (!(left.getDomain() instanceof TimeDomainSubset))
			throw new VTLIncompatibleTypesException("datediff", TIMEDS, left.getDomain());
		if (!(right.getDomain() instanceof TimeDomainSubset))
			throw new VTLIncompatibleTypesException("datediff", TIMEDS, left.getDomain());
		return INTEGER;
	}

	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetStructure dataset, ScalarValueMetadata<?, ?> scalar)
	{
		if (!(scalar.getDomain() instanceof TimeDomainSubset))
			throw new VTLIncompatibleTypesException("datediff", TIMEDS, scalar.getDomain());
		
		DataSetComponent<Measure, ?, ?> measure = dataset.getSingleton(Measure.class);
		if (!(measure.getDomain() instanceof TimeDomainSubset))
			throw new VTLIncompatibleTypesException("datediff", TIMEDS, measure);
		
		return new DataSetStructureBuilder(dataset).removeComponent(measure).addComponent(INT_VAR).build();
	}

	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(TransformationScheme scheme, DataSetStructure left, DataSetStructure right)
	{
		for (DataSetComponent<Measure, ?, ?> measure: List.of(left.getSingleton(Measure.class), right.getSingleton(Measure.class)))
			if (!(measure.getDomain() instanceof TimeDomainSubset))
				throw new VTLIncompatibleTypesException("datediff", TIMEDS, measure);
		
		return new DataSetStructureBuilder(left.getIDs()).addComponent(INT_VAR).build();
	}
	
	@Override
	public String toString()
	{
		return String.format("datediff(%s, %s)", getLeftOperand(), getRightOperand());
	}
}
