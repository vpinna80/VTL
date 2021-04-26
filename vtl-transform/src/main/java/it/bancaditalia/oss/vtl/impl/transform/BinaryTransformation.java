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
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
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

public abstract class BinaryTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	
	private final Transformation leftOperand, rightOperand;
	private transient VTLValueMetadata metadata = null;

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
		// Optimization, avoid parallelization of simple scalar operations
		if (scheme instanceof DatapointScope)
			return evalFinisher(leftOperand.eval(scheme), rightOperand.eval(scheme));
		else
			return ThreadUtils.evalFuture(scheme, this, this::evalFinisher, Transformation::eval);
	}
	
	@Override
	public final VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		return metadata == null ? metadata = ThreadUtils.evalFuture(scheme, this, this::getMetadataFinisher, Transformation::getMetadata) : metadata;
	}
	
	protected abstract VTLValue evalTwoScalars(ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right);

	protected abstract VTLValue evalDatasetWithScalar(boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar);

	protected abstract VTLValue evalTwoDatasets(DataSet left, DataSet right);

	protected abstract VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right);

	protected abstract VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?, ?> scalar);

	protected abstract VTLValueMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right);

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return Stream.concat(leftOperand.getTerminals().stream(), rightOperand.getTerminals().stream()).collect(toSet()); 
	}

	private VTLValue evalFinisher(VTLValue left, VTLValue right) 
	{
		if (left instanceof DataSet && right instanceof DataSet)
			return evalTwoDatasets((DataSet) left, (DataSet) right);
		else if (left instanceof DataSet && right instanceof ScalarValue)
			return evalDatasetWithScalar(true, (DataSet) left, (ScalarValue<?, ?, ?, ?>) right);
		else if (left instanceof ScalarValue && right instanceof DataSet)
			return evalDatasetWithScalar(false, (DataSet) right, (ScalarValue<?, ?, ?, ?>) left);
		else if (left instanceof ScalarValue && right instanceof ScalarValue)
			return evalTwoScalars((ScalarValue<?, ?, ?, ?>) left, (ScalarValue<?, ?, ?, ?>) right);
		else if (left instanceof DataSet || left instanceof ScalarValue)
			throw new VTLInvalidParameterException(right, DataSet.class, ScalarValue.class);
		else
			throw new VTLInvalidParameterException(left, DataSet.class, ScalarValue.class);
	}

	private VTLValueMetadata getMetadataFinisher(VTLValueMetadata left, VTLValueMetadata right) 
	{
		if (left instanceof UnknownValueMetadata || right instanceof UnknownValueMetadata)
			return INSTANCE;
		if (left instanceof DataSetMetadata && right instanceof DataSetMetadata)
			return getMetadataTwoDatasets((DataSetMetadata) left, (DataSetMetadata) right);
		else if (left instanceof DataSetMetadata && right instanceof ScalarValueMetadata)
			return getMetadataDatasetWithScalar(true, (DataSetMetadata) left, (ScalarValueMetadata<?, ?>) right);
		else if (left instanceof ScalarValueMetadata && right instanceof DataSetMetadata)
			return getMetadataDatasetWithScalar(false, (DataSetMetadata) right, (ScalarValueMetadata<?, ?>) left);
		else if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
			return getMetadataTwoScalars((ScalarValueMetadata<?, ?>) left, (ScalarValueMetadata<?, ?>) right);
		else if (left instanceof DataSetMetadata || left instanceof ScalarValueMetadata)
			throw new VTLInvalidParameterException(right, DataSetMetadata.class, ScalarValueMetadata.class);
		else
			throw new VTLInvalidParameterException(left, DataSetMetadata.class, ScalarValueMetadata.class);
	}

	public VTLValueMetadata getMetadata()
	{
		return metadata;
	}

	public Transformation getLeftOperand()
	{
		return leftOperand;
	}

	public Transformation getRightOperand()
	{
		return rightOperand;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((leftOperand == null) ? 0 : leftOperand.hashCode());
		result = prime * result + ((rightOperand == null) ? 0 : rightOperand.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!(obj instanceof BinaryTransformation)) return false;
		BinaryTransformation other = (BinaryTransformation) obj;
		if (leftOperand == null)
		{
			if (other.leftOperand != null) return false;
		}
		else if (!leftOperand.equals(other.leftOperand)) return false;
		if (rightOperand == null)
		{
			if (other.rightOperand != null) return false;
		}
		else if (!rightOperand.equals(other.rightOperand)) return false;
		return true;
	}
}
