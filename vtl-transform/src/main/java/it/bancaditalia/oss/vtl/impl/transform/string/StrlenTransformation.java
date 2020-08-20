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
package it.bancaditalia.oss.vtl.impl.transform.string;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class StrlenTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final VTLScalarValueMetadata<IntegerDomainSubset> META = () -> Domains.INTEGERDS;  
	
	public StrlenTransformation(Transformation operand)
	{
		super(operand);
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		return new IntegerValue((long) ((StringValue) scalar).get().length());
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		throw new UnsupportedOperationException("length() on dataset not implemented");
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata op = operand.getMetadata(session);
		if (op instanceof VTLScalarValueMetadata && ((VTLScalarValueMetadata<?>) op).getDomain() instanceof StringDomainSubset)
			return META;
		else
			throw new UnsupportedOperationException("String value expected.");
	}
	
	@Override
	public String toString() 
	{
		return "length(" + operand + ")";
	}
}
