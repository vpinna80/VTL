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
package it.bancaditalia.oss.vtl.impl.transform;

import static it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata.INSTANCE;
import static java.util.stream.Collectors.toSet;

import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.transform.util.ThreadUtils;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public abstract class BinaryTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	
	private final Transformation leftOperand, rightOperand;

	public BinaryTransformation(Transformation left, Transformation right)
	{
		this.leftOperand = left;
		this.rightOperand = right;
	}
	
	@Override
	public boolean isTerminal()
	{
		return false;
	}
	
	@Override
	public final VTLValue eval(TransformationScheme scheme)
	{
		try
		{
			// Optimization, avoid parallelization of simple scalar operations
			BinaryOperator<VTLValue> combiner = evalCombiner(getMetadata(scheme));
			if (Utils.SEQUENTIAL || scheme instanceof DatapointScope)
				return combiner.apply(leftOperand.eval(scheme), rightOperand.eval(scheme));
			else
				return ThreadUtils.evalFuture(combiner, t -> leftOperand.eval(scheme), t -> rightOperand.eval(scheme)).apply(this);
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("In expression " + this, e);
		}
	}
	
	protected final VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		try
		{
			return metadataCombiner(leftOperand.getMetadata(scheme), rightOperand.getMetadata(scheme));
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("In expression " + this, e);
		}
	}

	protected abstract VTLValue evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right);

	protected abstract VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar);

	protected abstract VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right);

	protected abstract VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right);

	protected abstract VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?, ?> scalar);

	protected abstract VTLValueMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right);

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return Stream.concat(leftOperand.getTerminals().stream(), rightOperand.getTerminals().stream()).collect(toSet()); 
	}

	private BinaryOperator<VTLValue> evalCombiner(VTLValueMetadata metadata) 
	{
		return (left, right) -> {
			if (left instanceof DataSet && right instanceof DataSet)
				return evalTwoDatasets(metadata, (DataSet) left, (DataSet) right);
			else if (left instanceof DataSet && right instanceof ScalarValue)
				return evalDatasetWithScalar(metadata, true, (DataSet) left, (ScalarValue<?, ?, ?, ?>) right);
			else if (left instanceof ScalarValue && right instanceof DataSet)
				return evalDatasetWithScalar(metadata, false, (DataSet) right, (ScalarValue<?, ?, ?, ?>) left);
			else // both scalars
				return evalTwoScalars(metadata, (ScalarValue<?, ?, ?, ?>) left, (ScalarValue<?, ?, ?, ?>) right);
		};
	}

	private VTLValueMetadata metadataCombiner(VTLValueMetadata left, VTLValueMetadata right) 
	{
		if (left instanceof UnknownValueMetadata || right instanceof UnknownValueMetadata)
			return INSTANCE;
		if (left instanceof DataSetMetadata && right instanceof DataSetMetadata)
			return getMetadataTwoDatasets((DataSetMetadata) left, (DataSetMetadata) right);
		else if (left instanceof DataSetMetadata && right instanceof ScalarValueMetadata)
			return getMetadataDatasetWithScalar(true, (DataSetMetadata) left, (ScalarValueMetadata<?, ?>) right);
		else if (left instanceof ScalarValueMetadata && right instanceof DataSetMetadata)
			return getMetadataDatasetWithScalar(false, (DataSetMetadata) right, (ScalarValueMetadata<?, ?>) left);
		else // both scalars
			return getMetadataTwoScalars((ScalarValueMetadata<?, ?>) left, (ScalarValueMetadata<?, ?>) right);
	}

	public Transformation getLeftOperand()
	{
		return leftOperand;
	}

	public Transformation getRightOperand()
	{
		return rightOperand;
	}
}
