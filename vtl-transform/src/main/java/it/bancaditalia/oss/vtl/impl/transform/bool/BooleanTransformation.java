/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static java.util.Collections.singletonMap;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class BooleanTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;

	public static enum BooleanBiOperator implements BinaryOperator<BooleanValue>
	{
		AND((l, r) -> BooleanValue.of(l.get() && r.get())), 
		OR((l, r) -> BooleanValue.of(l.get() || r.get())), 
		XOR((l, r) -> BooleanValue.of(l.get() ^ r.get()));

		private final BiFunction<BooleanValue, BooleanValue, BooleanValue> function;

		private BooleanBiOperator(BiFunction<BooleanValue, BooleanValue, BooleanValue> function)
		{
			this.function = function;
		}

		@Override
		public BooleanValue apply(BooleanValue left, BooleanValue right)
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
	private DataSetMetadata    metadata = null;

	public BooleanTransformation(BooleanBiOperator operator, Transformation left, Transformation right)
	{
		super(left, right);

		this.operator = operator;
	}

	@Override
	protected VTLValue evalTwoScalars(ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right)
	{
		return operator.apply((BooleanValue) left, (BooleanValue) right);
	}

	@Override
	protected VTLValue evalDatasetWithScalar(boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?> scalar)
	{
		DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> resultMeasure = metadata.getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, BooleanDomainSubset, BooleanDomain> datasetMeasure = dataset.getComponents(Measure.class, BOOLEANDS).iterator().next();

		return dataset.mapKeepingKeys(metadata, dp -> singletonMap(resultMeasure, operator.apply((BooleanValue) dp.get(datasetMeasure), (BooleanValue) scalar)));
	}

	@Override
	protected VTLValue evalTwoDatasets(DataSet left, DataSet right)
	{
		boolean leftHasMoreIdentifiers = left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class));

		DataSet streamed = leftHasMoreIdentifiers ? right : left;
		DataSet indexed = leftHasMoreIdentifiers ? left : right;
		DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> resultMeasure = metadata.getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, BooleanDomainSubset, BooleanDomain> indexedMeasure = indexed.getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, BooleanDomainSubset, BooleanDomain> streamedMeasure = streamed.getComponents(Measure.class, BOOLEANDS).iterator().next();

		// Scan the dataset with less identifiers and find the matches
		return indexed.filteredMappedJoin(metadata, streamed,
				(dp1, dp2) -> true,
				(dp1, dp2) -> new DataPointBuilder()
					.addAll(dp1.getValues(Identifier.class))
					.addAll(dp2.getValues(Identifier.class))
					.add(resultMeasure, dp1.get(indexedMeasure) instanceof NullValue || dp2.get(indexedMeasure) instanceof NullValue
								? NullValue.instance(BOOLEANDS)
								: operator.apply((BooleanValue) dp1.get(indexedMeasure), (BooleanValue) dp2.get(streamedMeasure)))
					.build(metadata));
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata left = leftOperand.getMetadata(session), right = rightOperand.getMetadata(session);

		if (left instanceof DataSetMetadata && right instanceof DataSetMetadata)
		{
			DataSetMetadata leftData = (DataSetMetadata) left, rightData = (DataSetMetadata) right;

			if (!leftData.getComponents(Identifier.class).containsAll(rightData.getComponents(Identifier.class))
					&& !rightData.getComponents(Identifier.class).containsAll(leftData.getComponents(Identifier.class)))
				throw new UnsupportedOperationException("One dataset must have all the identifiers of the other.");

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> leftMeasures = leftData.getComponents(Measure.class);
			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> rightMeasures = rightData.getComponents(Measure.class);

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
			
			return metadata = new DataStructureBuilder()
					.addComponents(leftData.getComponents(Identifier.class))
					.addComponents(rightData.getComponents(Identifier.class))
					.addComponent(new DataStructureComponentImpl<>(measureName, Measure.class, BOOLEANDS))
					.build();
		}
		else if (left instanceof ScalarValueMetadata && ((ScalarValueMetadata<?>) left).getDomain() instanceof BooleanDomainSubset && right instanceof ScalarValueMetadata
				&& ((ScalarValueMetadata<?>) right).getDomain() instanceof BooleanDomainSubset)
			return BOOLEAN;
		else
		{
			metadata = (DataSetMetadata) (left instanceof DataSetMetadata ? left : right);

			if (metadata.getComponents(Measure.class, Domains.BOOLEANDS).size() == 0)
				throw new VTLExpectedComponentException(Measure.class, Domains.BOOLEANDS, metadata);

			return metadata;
		}
	}

	@Override
	public String toString()
	{
		return leftOperand + " " + operator + " " + rightOperand;
	}
}
