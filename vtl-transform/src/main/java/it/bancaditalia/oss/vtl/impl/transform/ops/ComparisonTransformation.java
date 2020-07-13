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
package it.bancaditalia.oss.vtl.impl.transform.ops;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static java.util.Collections.singletonMap;

import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleMeasuresException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointImpl.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureImpl.Builder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.operators.ComparisonOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class ComparisonTransformation extends BinaryTransformation
{
	private static final Logger LOGGER = LoggerFactory.getLogger(JoinTransformation.class);
	private static final long serialVersionUID = 1L;

	private final ComparisonOperator operator;

	private VTLDataSetMetadata metadata;
	private boolean castToLeft = false;

	public ComparisonTransformation(ComparisonOperator operator, Transformation left, Transformation right)
	{
		super(left, right);

		this.operator = operator;
	}

	@Override
	protected VTLValue evalTwoScalars(ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return BooleanValue.FALSE;

		if (castToLeft)
			right = left.getDomain().cast(right);
		else
			left = right.getDomain().cast(left);

		return operator.apply(left, right);
	}

	@Override
	protected VTLValue evalDatasetWithScalar(boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?> scalar)
	{
		DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> resultMeasure = metadata.getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getComponents(Measure.class).iterator().next();

		final ScalarValue<?, ?, ?> castedScalar;
		if (castToLeft && datasetIsLeftOp)
			castedScalar = measure.cast(scalar);
		else
			castedScalar = scalar;
		
		if (castToLeft) 
			if (datasetIsLeftOp)
				return dataset.mapKeepingKeys(metadata, dp -> singletonMap(resultMeasure, operator.apply(dp.get(measure), castedScalar)));
			else
				return dataset.mapKeepingKeys(metadata, dp -> singletonMap(resultMeasure, operator.apply(scalar, scalar.getDomain().cast(dp.get(measure)))));
		else
			if (datasetIsLeftOp)
				return dataset.mapKeepingKeys(metadata, dp -> singletonMap(resultMeasure, operator.apply(scalar.getDomain().cast(dp.get(measure)), scalar)));
			else
				return dataset.mapKeepingKeys(metadata, dp -> singletonMap(resultMeasure, operator.apply(castedScalar, dp.get(measure))));
	}

	@Override
	protected VTLValue evalTwoDatasets(DataSet left, DataSet right)
	{
		boolean leftHasMoreIdentifiers = left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class));

		DataSet streamed = leftHasMoreIdentifiers ? right : left;
		DataSet indexed = leftHasMoreIdentifiers ? left : right;
		DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> resultMeasure = metadata.getComponents(Measure.class, BOOLEANDS).iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> indexedMeasure = indexed.getComponents(Measure.class).iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> streamedMeasure = streamed.getComponents(Measure.class).iterator().next();
		
		// must remember which is the left operand because some operators are not commutative, also cast
		BiFunction<ScalarValue<?, ?, ?>, ScalarValue<?, ?, ?>, ScalarValue<?, BooleanDomainSubset, BooleanDomain>> casted = (a, b) -> castToLeft ? operator.apply(a, a.getDomain().cast(b)) : operator.apply(b.getDomain().cast(a), b);
		BiFunction<ScalarValue<?, ?, ?>, ScalarValue<?, ?, ?>, ScalarValue<?, BooleanDomainSubset, BooleanDomain>> function =
				Utils.reverseIf(leftHasMoreIdentifiers, casted);

		// Scan the dataset with less identifiers and find the matches
		return streamed.filteredMappedJoin(metadata, indexed,
						(dps, dpi) -> true,
						(dps, dpi) -> new DataPointBuilder()
								.addAll(dps.getValues(Identifier.class))
								.addAll(dpi.getValues(Identifier.class))
								.add(resultMeasure, function.apply(dps.get(indexedMeasure), dpi.get(streamedMeasure)))
								.build(metadata));
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		// TODO: BETTER CHECKS

		// else if (!l.getDomain().isAssignableFrom(r.getDomain()) && !r.getDomain().isAssignableFrom(l.getDomain()))
		// return BooleanValue.FALSE;
		// else if (op != VTL.EQUAL && op != VTL.DIAMOND && !l.getDomain().isComparableWith(r.getDomain()))
		// throw new UnsupportedOperationException("Operators are of incompatible domains in comparison: " + l.getDomain() + ",
		// " + r.getDomain());

		if (metadata != null)
			return metadata;
		
		VTLValueMetadata left = leftOperand.getMetadata(session), right = rightOperand.getMetadata(session); 
		castToLeft = false;
		
		if (left instanceof VTLScalarValueMetadata && right instanceof VTLScalarValueMetadata)
		{
			VTLScalarValueMetadata<?> scalarLeft = (VTLScalarValueMetadata<?>) left;
			VTLScalarValueMetadata<?> scalarRight = (VTLScalarValueMetadata<?>) right;

			castToLeft = scalarLeft.getDomain().isAssignableFrom(scalarRight.getDomain()); 
			if (castToLeft || scalarRight.getDomain().isAssignableFrom(scalarLeft.getDomain())) 
				return BOOLEAN;
			else
				throw new VTLIncompatibleTypesException("comparison branch", scalarLeft.getDomain(), ((VTLScalarValueMetadata<?>) right).getDomain());
		}
		else if (left instanceof VTLDataSetMetadata && right instanceof VTLScalarValueMetadata ||
				right instanceof VTLDataSetMetadata && left instanceof VTLScalarValueMetadata)
		{
			boolean leftIsDataset = left instanceof VTLDataSetMetadata;
			VTLDataSetMetadata ds = leftIsDataset ? (VTLDataSetMetadata) left : (VTLDataSetMetadata) right;
			ValueDomainSubset<?> scalarDomain = (leftIsDataset ? (VTLScalarValueMetadata<?>) right : (VTLScalarValueMetadata<?>) left).getDomain();

			if (ds.getComponents(Measure.class).size() != 1)
				throw new VTLExpectedComponentException(Measure.class, ds);
			DataStructureComponent<?, ?, ?> measure = ds.getComponents(Measure.class).iterator().next();
			
			if (leftIsDataset)
				castToLeft = measure.getDomain().isAssignableFrom(scalarDomain);
			else
				castToLeft = scalarDomain.isAssignableFrom(measure.getDomain());

			if (!castToLeft && (leftIsDataset && !scalarDomain.isAssignableFrom(measure.getDomain())
					|| !leftIsDataset && !measure.getDomain().isAssignableFrom(scalarDomain)))
				throw new VTLIncompatibleTypesException("comparison condition", measure, scalarDomain);
			
			return metadata = new Builder().addComponents(ds.getComponents(Identifier.class))
					.addComponents(new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS)).build();
		}
		else if (left instanceof VTLDataSetMetadata && right instanceof VTLDataSetMetadata)
		{
			VTLDataSetMetadata dsLeft = (VTLDataSetMetadata) left, dsRight = (VTLDataSetMetadata) right;
			
			LOGGER.info("Comparing {} to {}", dsLeft, dsRight);
			
			if (dsLeft.getComponents(Measure.class).size() != 1)
				throw new VTLExpectedComponentException(Measure.class, dsLeft.getComponents(Measure.class));
			if (dsRight.getComponents(Measure.class).size() != 1)
				throw new VTLExpectedComponentException(Measure.class, dsRight.getComponents(Measure.class));
			
			if (!dsLeft.getComponents(Identifier.class).containsAll(dsRight.getComponents(Identifier.class)) 
					&& !dsRight.getComponents(Identifier.class).containsAll(dsLeft.getComponents(Identifier.class)))
				throw new UnsupportedOperationException("One operand of comparison must contain all identifiers of the other.");

			final DataStructureComponent<? extends Measure, ?, ?> leftMeasure = dsLeft.getComponents(Measure.class).iterator().next(),
					rightMeasure = dsLeft.getComponents(Measure.class).iterator().next();
			
			if (leftMeasure.getDomain().isAssignableFrom(rightMeasure.getDomain()))
				castToLeft = true;
			else if (!rightMeasure.getDomain().isAssignableFrom(leftMeasure.getDomain()))
				throw new VTLIncompatibleMeasuresException("comparison", leftMeasure, rightMeasure);

			return metadata = new Builder()
					.addComponents(dsLeft.getComponents(Identifier.class))
					.addComponents(dsRight.getComponents(Identifier.class))
					.addComponents(new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS))
					.build();
		}

		throw new UnsupportedOperationException("Found invalid parameters in comparison: " + left + ", " + right);
	}
	
	@Override
	public String toString()
	{
		return leftOperand + " " + operator + " " + rightOperand;
	}
}
