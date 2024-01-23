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

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public abstract class UnaryTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	protected final Transformation operand;

	public UnaryTransformation(Transformation operand)
	{
		this.operand = requireNonNull(operand);
	}
	
	@Override
	public final boolean isTerminal()
	{
		return operand != null;
	}
	
	@Override
	public final Set<LeafTransformation> getTerminals()
	{
		return operand != null ? operand.getTerminals() : emptySet();
	}
	
	@Override
	public final VTLValue eval(TransformationScheme scheme)
	{
		VTLValue value = operand == null ? scheme.resolve(THIS) : operand.eval(scheme);
		
		if (value instanceof DataSet)
			return evalOnDataset((DataSet) value, getMetadata(scheme));
		else
			return evalOnScalar((ScalarValue<?, ?, ?, ?>) value, getMetadata(scheme));
	}

	public Transformation getOperand()
	{
		return operand;
	}

	protected abstract VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata);

	protected abstract VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata);
}
