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

import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.FALSE;
import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineage2Enricher;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.Collections.singletonMap;

import java.util.function.BinaryOperator;

import it.bancaditalia.oss.vtl.exceptions.VTLExpectedRoleException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;

public class BooleanTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;

	public static enum BooleanBiOperator implements BinaryOperator<ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain>>
	{
		AND(BooleanBiOperator::and), 
		OR(BooleanBiOperator::or), 
		XOR(BooleanBiOperator::xor);

		private final BinaryOperator<ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain>> function;

		private BooleanBiOperator(BinaryOperator<ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain>> function)
		{
			this.function = function;
		}

		@Override
		public ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> apply(ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> left, ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> right)
		{
			return function.apply(left, right);
		}
		
		@Override
		public String toString()
		{
			return name().toLowerCase();
		}
		
		public static ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> and(ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> left, ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> right)
		{
			if (left == FALSE || right == FALSE)
				return FALSE;
			else if (left.isNull() || right.isNull())
				return BooleanValue.NULL;
			else 
				return TRUE; 
		}
		
		public static ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> or(ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> left, ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> right)
		{
			if (left == TRUE || right == TRUE)
				return TRUE;
			else if (left.isNull() || right.isNull())
				return BooleanValue.of(null);
			else 
				return FALSE; 
		}
		
		public static ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> xor(ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> left, ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> right)
		{
			if (left == TRUE && right == TRUE)
				return FALSE;
			else if (left == TRUE || right == TRUE)
				return TRUE;
			else if (left.isNull() || right.isNull())
				return BooleanValue.of(null);
			else 
				return FALSE; 
		}
	}

	private final BooleanBiOperator operator;

	public BooleanTransformation(BooleanBiOperator operator, Transformation left, Transformation right)
	{
		super(left, right);

		this.operator = operator;
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		return evalTwoScalars(left, right);
	}

	private ScalarValue<?, ?, ?, ?> evalTwoScalars(ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		return operator.apply(BOOLEANDS.cast(left), BOOLEANDS.cast(right));
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		DataSetComponent<Measure, ?, ?> resultMeasure = ((DataSetStructure) metadata).getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataSetComponent<? extends Measure, ?, ?> datasetMeasure = dataset.getMetadata().getComponents(Measure.class, BOOLEANDS).iterator().next();

		SerBinaryOperator<ScalarValue<?, ?, ?, ?>> evalTwoScalars = this::evalTwoScalars;
		SerBinaryOperator<ScalarValue<?, ?, ?, ?>> reversedIf = evalTwoScalars.reverseIf(!datasetIsLeftOp);

		return dataset.mapKeepingKeys((DataSetStructure) metadata, lineageEnricher(this), 
				dp -> singletonMap(resultMeasure, reversedIf.apply(dp.get(datasetMeasure), scalar)));
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		boolean leftHasMoreIdentifiers = left.getMetadata().getIDs().containsAll(right.getMetadata().getIDs());

		DataSet streamed = leftHasMoreIdentifiers ? right : left;
		DataSet indexed = leftHasMoreIdentifiers ? left : right;
		DataSetComponent<Measure, ?, ?> resultMeasure = ((DataSetStructure) metadata).getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataSetComponent<? extends Measure, ?, ?> indexedMeasure = indexed.getMetadata().getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataSetComponent<? extends Measure, ?, ?> streamedMeasure = streamed.getMetadata().getComponents(Measure.class, BOOLEANDS).iterator().next();

		SerBinaryOperator<ScalarValue<?, ?, ?, ?>> evalTwoScalars = this::evalTwoScalars;
		SerBinaryOperator<ScalarValue<?, ?, ?, ?>> reversedIf = evalTwoScalars.reverseIf(leftHasMoreIdentifiers);
		
		// Scan the dataset with less identifiers and find the matches
		SerBinaryOperator<Lineage> enricher = lineage2Enricher(this);
		return streamed.mappedJoin((DataSetStructure) metadata, indexed, (dps, dpi) -> new DataPointBuilder()
					.addAll(dps.getValues(Identifier.class))
					.addAll(dpi.getValues(Identifier.class))
					.add(resultMeasure, reversedIf.apply(dps.get(streamedMeasure), dpi.get(indexedMeasure)))
					.build(enricher.apply(dps.getLineage(), dpi.getLineage()), (DataSetStructure) metadata));
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		if (!BOOLEANDS.isAssignableFrom(left.getDomain()))
			throw new VTLIncompatibleTypesException(operator.toString(), left.getDomain(), BOOLEANDS);
		else if (!BOOLEANDS.isAssignableFrom(right.getDomain()))
			throw new VTLIncompatibleTypesException(operator.toString(), right.getDomain(), BOOLEANDS);
		else
			return BOOLEAN;
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean b, DataSetStructure dataset, ScalarValueMetadata<?, ?> right)
	{
		if (!BOOLEANDS.isAssignableFrom(right.getDomain()))
			throw new VTLIncompatibleTypesException(operator.toString(), right.getDomain(), BOOLEANDS);
		else if (dataset.getComponents(Measure.class, Domains.BOOLEANDS).size() == 0)
			throw new VTLExpectedRoleException(Measure.class, Domains.BOOLEANDS, dataset);
		else
			return dataset;
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(DataSetStructure left, DataSetStructure right)
	{
		if (!left.getIDs().containsAll(right.getIDs()) && !right.getIDs().containsAll(left.getIDs()))
			throw new UnsupportedOperationException("One dataset must have all the identifiers of the other.");

		DataSetComponent<? extends Measure, ?, ?> leftMeasure = left.getSingleton(Measure.class);
		DataSetComponent<? extends Measure, ?, ?> rightMeasure = right.getSingleton(Measure.class);

		if (!BOOLEANDS.isAssignableFrom(leftMeasure.getDomain()))
			throw new UnsupportedOperationException("Expected boolean measure but found: " + leftMeasure);
		if (!BOOLEANDS.isAssignableFrom(rightMeasure.getDomain()))
			throw new UnsupportedOperationException("Expected boolean measure but found: " + rightMeasure);

		DataSetStructureBuilder builder = new DataSetStructureBuilder()
				.addComponents(left.getIDs())
				.addComponents(right.getIDs());
		
		if (leftMeasure.getAlias().equals(rightMeasure.getAlias()))
			builder.addComponent(leftMeasure);
		else
			builder.addComponent(BOOL_VAR);
		
		return builder.build();
	}

	@Override
	public String toString()
	{
		return getLeftOperand() + " " + operator + " " + getRightOperand();
	}
}
