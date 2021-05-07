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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static java.util.Collections.singletonMap;

import java.util.Set;
import java.util.function.BinaryOperator;

import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.Utils;

public class BooleanTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;

	public static enum BooleanBiOperator implements BinaryOperator<ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain>>
	{
		AND(BooleanValue::and), 
		OR(BooleanValue::or), 
		XOR(BooleanValue::xor);

		private final BinaryOperator<ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain>> function;

		private BooleanBiOperator(BinaryOperator<ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain>> function)
		{
			this.function = function;
		}

		@Override
		public ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> apply(ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> left, ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> right)
		{
			return function.apply(left, right);
		}
		
		@Override
		public String toString()
		{
			return name().toLowerCase();
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
		DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> resultMeasure = ((DataSetMetadata) metadata).getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, EntireBooleanDomainSubset, BooleanDomain> datasetMeasure = dataset.getComponents(Measure.class, BOOLEANDS).iterator().next();

		return dataset.mapKeepingKeys((DataSetMetadata) metadata, dp -> LineageNode.of(this, dp.getLineage()), dp -> singletonMap(resultMeasure, Utils.<ScalarValue<?, ?, ?, ?>>reverseIf(this::evalTwoScalars, !datasetIsLeftOp).apply(dp.get(datasetMeasure), scalar)));
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		boolean leftHasMoreIdentifiers = left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class));

		DataSet streamed = leftHasMoreIdentifiers ? right : left;
		DataSet indexed = leftHasMoreIdentifiers ? left : right;
		DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> resultMeasure = ((DataSetMetadata) metadata).getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, EntireBooleanDomainSubset, BooleanDomain> indexedMeasure = indexed.getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, EntireBooleanDomainSubset, BooleanDomain> streamedMeasure = streamed.getComponents(Measure.class, BOOLEANDS).iterator().next();

		// Scan the dataset with less identifiers and find the matches
		return streamed.mappedJoin((DataSetMetadata) metadata, indexed,
				(dps, dpi) -> new DataPointBuilder()
					.addAll(dps.getValues(Identifier.class))
					.addAll(dpi.getValues(Identifier.class))
					.add(resultMeasure, Utils.<ScalarValue<?, ?, ?, ?>>reverseIf(this::evalTwoScalars, leftHasMoreIdentifiers).apply(dps.get(streamedMeasure), dpi.get(indexedMeasure)))
					.build(getLineage(), (DataSetMetadata) metadata));
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
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean b, DataSetMetadata dataset, ScalarValueMetadata<?, ?> right)
	{
		if (!BOOLEANDS.isAssignableFrom(right.getDomain()))
			throw new VTLIncompatibleTypesException(operator.toString(), right.getDomain(), BOOLEANDS);
		else if (dataset.getComponents(Measure.class, Domains.BOOLEANDS).size() == 0)
			throw new VTLExpectedComponentException(Measure.class, Domains.BOOLEANDS, dataset);
		else
			return dataset;
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right)
	{
		if (!left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class))
				&& !right.getComponents(Identifier.class).containsAll(left.getComponents(Identifier.class)))
			throw new UnsupportedOperationException("One dataset must have all the identifiers of the other.");

		Set<? extends DataStructureComponent<? extends Measure, ?, ?>> leftMeasures = left.getComponents(Measure.class);
		Set<? extends DataStructureComponent<? extends Measure, ?, ?>> rightMeasures = right.getComponents(Measure.class);

		if (leftMeasures.size() != 1)
			throw new UnsupportedOperationException("Expected single boolean measure but found: " + leftMeasures);
		if (rightMeasures.size() != 1)
			throw new UnsupportedOperationException("Expected single boolean measure but found: " + rightMeasures);

		DataStructureComponent<? extends Measure, ?, ?> leftMeasure = leftMeasures.iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> rightMeasure = rightMeasures.iterator().next();

		if (!BOOLEANDS.isAssignableFrom(leftMeasure.getDomain()))
			throw new UnsupportedOperationException("Expected boolean measure but found: " + leftMeasure);
		if (!BOOLEANDS.isAssignableFrom(rightMeasure.getDomain()))
			throw new UnsupportedOperationException("Expected boolean measure but found: " + rightMeasure);

		String measureName = leftMeasure.getName().equals(rightMeasure.getName()) ? leftMeasure.getName() : "bool_var";
		
		return new DataStructureBuilder()
				.addComponents(left.getComponents(Identifier.class))
				.addComponents(right.getComponents(Identifier.class))
				.addComponent(new DataStructureComponentImpl<>(measureName, Measure.class, BOOLEANDS))
				.build();
	}

	@Override
	public String toString()
	{
		return getLeftOperand() + " " + operator + " " + getRightOperand();
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof BooleanTransformation)) return false;
		BooleanTransformation other = (BooleanTransformation) obj;
		if (operator != other.operator) return false;
		return true;
	}
}
