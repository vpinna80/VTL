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

import static java.util.stream.Collectors.toSet;

import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public abstract class BinaryTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	protected final Transformation leftOperand, rightOperand;

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
	public final VTLValue eval(TransformationScheme session)
	{
		VTLValue left = leftOperand.eval(session);
		VTLValue right = rightOperand.eval(session);
		
		if (left instanceof DataSet && right instanceof DataSet)
			return evalTwoDatasets((DataSet) left, (DataSet) right);
		else if (left instanceof DataSet && right instanceof ScalarValue)
			return evalDatasetWithScalar(true, (DataSet) left, (ScalarValue<?, ?, ?>) right);
		else if (left instanceof ScalarValue && right instanceof DataSet)
			return evalDatasetWithScalar(false, (DataSet) right, (ScalarValue<?, ?, ?>) left);
		else if (left instanceof ScalarValue && right instanceof ScalarValue)
			return evalTwoScalars((ScalarValue<?, ?, ?>) left, (ScalarValue<?, ?, ?>) right);
		else if (left instanceof DataSet || left instanceof ScalarValue)
			throw new VTLInvalidParameterException(right, DataSet.class, ScalarValue.class);
		else
			throw new VTLInvalidParameterException(left, DataSet.class, ScalarValue.class);
	}
	
	protected abstract VTLValue evalTwoScalars(ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right);

	protected abstract VTLValue evalDatasetWithScalar(boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?> scalar);

	protected abstract VTLValue evalTwoDatasets(DataSet left, DataSet right);

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return Stream.concat(leftOperand.getTerminals().stream(), rightOperand.getTerminals().stream()).collect(toSet()); 
	}
}
