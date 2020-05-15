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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;

import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;

public class DoubleValue extends NumberValueImpl<Double, NumberDomainSubset<NumberDomain>, NumberDomain>
{
	private static final long serialVersionUID = 1L;
	private static final VTLScalarValueMetadata<NumberDomainSubset<NumberDomain>> NUMBER_METADATA = () -> NUMBERDS;

	public DoubleValue(Double value)
	{
		super(value, Domains.NUMBERDS);
	}

	@Override
	public VTLScalarValueMetadata<NumberDomainSubset<NumberDomain>> getMetadata()
	{
		return NUMBER_METADATA;
	}
}
