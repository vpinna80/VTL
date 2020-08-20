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
package it.bancaditalia.oss.vtl.impl.types.data;

import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;

public class IntegerValue extends NumberValueImpl<Long, IntegerDomainSubset, IntegerDomain>
{
	private static final long serialVersionUID = 1L;
	private static final VTLScalarValueMetadata<IntegerDomainSubset> META = () -> Domains.INTEGERDS;

	public IntegerValue(Long value)
	{
		super(value, Domains.INTEGERDS);
	}

	public DoubleValue asDoubleValue()
	{
		return new DoubleValue(get().doubleValue());
	}
	
	@Override
	public VTLScalarValueMetadata<IntegerDomainSubset> getMetadata()
	{
		return META;
	}
}
