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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineagesEnricher;
import static java.util.Collections.singletonMap;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.operators.ComparisonOperator;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class ComparisonTransformation extends BinaryTransformation
{
	private static final Logger LOGGER = LoggerFactory.getLogger(JoinTransformation.class);
	private static final long serialVersionUID = 1L;

	private final ComparisonOperator operator;
	
	public ComparisonTransformation(ComparisonOperator operator, Transformation left, Transformation right)
	{
		super(left, right);

		this.operator = operator;
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalTwoScalars(VTLValueMetadata resultMetadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left.isNull() || right.isNull())
			return NullValue.instance(BOOLEANDS);

		if (left.getDomain().isAssignableFrom(right.getDomain()))
			right = left.getDomain().cast(right);
		else
			left = right.getDomain().cast(left);

		return operator.apply(left, right);
	}

	@Override
	protected DataSet evalDatasetWithScalar(VTLValueMetadata resultMetadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		DataSetComponent<Measure, ?, ?> resultMeasure = ((DataSetStructure) resultMetadata).getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataSetComponent<? extends Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		
		boolean castToLeft;
		if (datasetIsLeftOp)
			castToLeft = measure.getDomain().isAssignableFrom(scalar.getDomain());
		else
			castToLeft = scalar.getDomain().isAssignableFrom(measure.getDomain());

		ScalarValue<?, ?, ?, ?> castedScalar;
		if (castToLeft && datasetIsLeftOp)
			castedScalar = measure.getDomain().cast(scalar);
		else
			castedScalar = scalar;

		SerFunction<DataPoint, ScalarValue<?, ?, ?, ?>> extractor;
		if (castToLeft) 
			if (datasetIsLeftOp)
				extractor = dp -> operator.apply(dp.get(measure), castedScalar);
			else
				extractor = dp -> operator.apply(scalar, scalar.getDomain().cast(dp.get(measure)));
		else
			if (datasetIsLeftOp)
				extractor = dp -> operator.apply(scalar.getDomain().cast(dp.get(measure)), scalar);
			else
				extractor = dp -> operator.apply(castedScalar, dp.get(measure));

		SerUnaryOperator<Lineage> enricher = lineageEnricher(this);
		return dataset.mapKeepingKeys((DataSetStructure) resultMetadata, 
				lineage -> enricher.apply(lineage), 
				dp -> singletonMap(resultMeasure, extractor.apply(dp)));
	}

	@Override
	protected DataSet evalTwoDatasets(VTLValueMetadata resultMetadata, DataSet left, DataSet right)
	{
		DataSetComponent<Measure, ?, ?> resultMeasure = ((DataSetStructure) resultMetadata).getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataSetComponent<? extends Measure, ?, ?> lMeasure = left.getMetadata().getMeasures().iterator().next();
		DataSetComponent<? extends Measure, ?, ?> rMeasure = right.getMetadata().getMeasures().iterator().next();
		
		// must remember which is the left operand because some operators are not commutative, also cast
		ValueDomainSubset<?, ?> lDomain = lMeasure.getDomain();
		ValueDomainSubset<?, ?> rDomain = rMeasure.getDomain();
		SerBinaryOperator<ScalarValue<?, ?, ?, ?>> casted;
		if (lDomain.isAssignableFrom(rDomain))
			casted = (l, r) -> operator.apply(l, lDomain.cast(r));
		else
			casted = (l, r) -> operator.apply(rDomain.cast(l), r);
		
		SerFunction<Collection<Lineage>, Lineage> enricher = lineagesEnricher(this);
		return left.mappedJoin((DataSetStructure) resultMetadata, right, (dpl, dpr) -> new DataPointBuilder()
				.addAll(dpl.getValues(Identifier.class))
				.addAll(dpr.getValues(Identifier.class))
				.add(resultMeasure, casted.apply(dpl.get(lMeasure), dpr.get(rMeasure)))
				.build(enricher.apply(List.of(dpl.getLineage(), dpr.getLineage())), (DataSetStructure) resultMetadata));
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		if (left.getDomain().isAssignableFrom(right.getDomain()) || right.getDomain().isAssignableFrom(left.getDomain())) 
			return BOOLEAN;
		else
			throw new VTLIncompatibleTypesException("comparison branch", left.getDomain(), right.getDomain());
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetStructure dataset, ScalarValueMetadata<?, ?> scalar)
	{
		ValueDomainSubset<?, ?> scalarDomain = scalar.getDomain();
		DataSetComponent<?, ?, ?> measure = dataset.getSingleton(Measure.class);
		
		boolean castToLeft;
		if (datasetIsLeftOp)
			castToLeft = measure.getDomain().isAssignableFrom(scalarDomain);
		else
			castToLeft = scalarDomain.isAssignableFrom(measure.getDomain());

		if (!castToLeft && (datasetIsLeftOp && !scalarDomain.isAssignableFrom(measure.getDomain())
				|| !datasetIsLeftOp && !measure.getDomain().isAssignableFrom(scalarDomain)))
			throw new VTLIncompatibleTypesException("comparison condition", measure, scalarDomain);
		
		return new DataSetStructureBuilder().addComponents(dataset.getIDs())
				.addComponents(BOOL_VAR).build();
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(TransformationScheme scheme, DataSetStructure left, DataSetStructure right)
	{
		LOGGER.info("Comparing {} to {}", left, right);
		
		if (left.getMeasures().size() != 1)
			throw new VTLSingletonComponentRequiredException(Measure.class, left);
		if (right.getMeasures().size() != 1)
			throw new VTLSingletonComponentRequiredException(Measure.class, right);
		
		if (!left.getIDs().containsAll(right.getIDs()) 
				&& !right.getIDs().containsAll(left.getIDs()))
			throw new UnsupportedOperationException("Identifiers do not match: " + left.getIDs() + " and " + right.getIDs());

		final DataSetComponent<? extends Measure, ?, ?> leftMeasure = left.getMeasures().iterator().next(),
				rightMeasure = left.getMeasures().iterator().next();
		
		if (!leftMeasure.getDomain().isAssignableFrom(rightMeasure.getDomain()) && 
				!rightMeasure.getDomain().isAssignableFrom(leftMeasure.getDomain()))
			throw new VTLIncompatibleTypesException("comparison", leftMeasure, rightMeasure);

		return new DataSetStructureBuilder()
				.addComponents(left.getIDs())
				.addComponents(right.getIDs())
				.addComponents(BOOL_VAR)
				.build();
	}
	
	@Override
	public String toString()
	{
		return getLeftOperand() + " " + operator + " " + getRightOperand();
	}
}
