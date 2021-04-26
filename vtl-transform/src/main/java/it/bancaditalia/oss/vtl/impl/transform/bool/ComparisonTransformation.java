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

import java.util.function.BinaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleMeasuresException;
import it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.operators.ComparisonOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.Utils;

public class ComparisonTransformation extends BinaryTransformation
{
	private static final Logger LOGGER = LoggerFactory.getLogger(JoinTransformation.class);
	private static final long serialVersionUID = 1L;

	private final ComparisonOperator operator;
	
	private transient boolean castToLeft = false;

	public ComparisonTransformation(ComparisonOperator operator, Transformation left, Transformation right)
	{
		super(left, right);

		this.operator = operator;
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return NullValue.instance(BOOLEANDS);

		if (castToLeft)
			right = left.getDomain().cast(right);
		else
			left = right.getDomain().cast(left);

		return operator.apply(left, right);
	}

	@Override
	protected DataSet evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> resultMeasure = ((DataSetMetadata) metadata).getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getComponents(Measure.class).iterator().next();
		
		final ScalarValue<?, ?, ?, ?> castedScalar;
		if (castToLeft && datasetIsLeftOp)
			castedScalar = measure.cast(scalar);
		else
			castedScalar = scalar;
		
		if (castToLeft) 
			if (datasetIsLeftOp)
				return dataset.mapKeepingKeys((DataSetMetadata) metadata, dp -> singletonMap(resultMeasure, operator.apply(dp.get(measure), castedScalar)));
			else
				return dataset.mapKeepingKeys((DataSetMetadata) metadata, dp -> singletonMap(resultMeasure, operator.apply(scalar, scalar.getDomain().cast(dp.get(measure)))));
		else
			if (datasetIsLeftOp)
				return dataset.mapKeepingKeys((DataSetMetadata) metadata, dp -> singletonMap(resultMeasure, operator.apply(scalar.getDomain().cast(dp.get(measure)), scalar)));
			else
				return dataset.mapKeepingKeys((DataSetMetadata) metadata, dp -> singletonMap(resultMeasure, operator.apply(castedScalar, dp.get(measure))));
	}

	@Override
	protected DataSet evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		boolean leftHasMoreIdentifiers = left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class));

		DataSet streamed = leftHasMoreIdentifiers ? right : left;
		DataSet indexed = leftHasMoreIdentifiers ? left : right;
		DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> resultMeasure = ((DataSetMetadata) metadata).getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> indexedMeasure = indexed.getComponents(Measure.class).iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> streamedMeasure = streamed.getComponents(Measure.class).iterator().next();
		
		// must remember which is the left operand because some operators are not commutative, also cast
		BinaryOperator<ScalarValue<?, ?, ?, ?>> casted = (a, b) -> castToLeft ? operator.apply(a, a.getDomain().cast(b)) : operator.apply(b.getDomain().cast(a), b);
		BinaryOperator<ScalarValue<?, ?, ?, ?>> function = Utils.reverseIf(casted, leftHasMoreIdentifiers);

		// Scan the dataset with less identifiers and find the matches
		return streamed.filteredMappedJoin((DataSetMetadata) metadata, indexed,
				(dps, dpi) -> true,
				(dps, dpi) -> new DataPointBuilder()
						.addAll(dps.getValues(Identifier.class))
						.addAll(dpi.getValues(Identifier.class))
						.add(resultMeasure, function.apply(dps.get(streamedMeasure), dpi.get(indexedMeasure)))
						.build((DataSetMetadata) metadata));
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		castToLeft = left.getDomain().isAssignableFrom(right.getDomain()); 
		if (castToLeft || right.getDomain().isAssignableFrom(left.getDomain())) 
			return BOOLEAN;
		else
			throw new VTLIncompatibleTypesException("comparison branch", left.getDomain(), right.getDomain());
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?, ?> scalar)
	{
		ValueDomainSubset<?, ?> scalarDomain = scalar.getDomain();

		if (dataset.getComponents(Measure.class).size() != 1)
			throw new VTLExpectedComponentException(Measure.class, dataset);
		DataStructureComponent<?, ?, ?> measure = dataset.getComponents(Measure.class).iterator().next();
		
		if (datasetIsLeftOp)
			castToLeft = measure.getDomain().isAssignableFrom(scalarDomain);
		else
			castToLeft = scalarDomain.isAssignableFrom(measure.getDomain());

		if (!castToLeft && (datasetIsLeftOp && !scalarDomain.isAssignableFrom(measure.getDomain())
				|| !datasetIsLeftOp && !measure.getDomain().isAssignableFrom(scalarDomain)))
			throw new VTLIncompatibleTypesException("comparison condition", measure, scalarDomain);
		
		return new DataStructureBuilder().addComponents(dataset.getComponents(Identifier.class))
				.addComponents(new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS)).build();
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right)
	{
		LOGGER.info("Comparing {} to {}", left, right);
		
		if (left.getComponents(Measure.class).size() != 1)
			throw new VTLExpectedComponentException(Measure.class, left.getComponents(Measure.class));
		if (right.getComponents(Measure.class).size() != 1)
			throw new VTLExpectedComponentException(Measure.class, right.getComponents(Measure.class));
		
		if (!left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class)) 
				&& !right.getComponents(Identifier.class).containsAll(left.getComponents(Identifier.class)))
			throw new UnsupportedOperationException("One operand of comparison must contain all identifiers of the other.");

		final DataStructureComponent<? extends Measure, ?, ?> leftMeasure = left.getComponents(Measure.class).iterator().next(),
				rightMeasure = left.getComponents(Measure.class).iterator().next();
		
		if (leftMeasure.getDomain().isAssignableFrom(rightMeasure.getDomain()))
			castToLeft = true;
		else if (!rightMeasure.getDomain().isAssignableFrom(leftMeasure.getDomain()))
			throw new VTLIncompatibleMeasuresException("comparison", leftMeasure, rightMeasure);

		return new DataStructureBuilder()
				.addComponents(left.getComponents(Identifier.class))
				.addComponents(right.getComponents(Identifier.class))
				.addComponents(new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS))
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
		if (!(obj instanceof ComparisonTransformation)) return false;
		ComparisonTransformation other = (ComparisonTransformation) obj;
		if (operator != other.operator) return false;
		return true;
	}
}
