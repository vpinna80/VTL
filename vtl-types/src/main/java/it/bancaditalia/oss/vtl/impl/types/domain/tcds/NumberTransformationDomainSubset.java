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
package it.bancaditalia.oss.vtl.impl.types.domain.tcds;

import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class NumberTransformationDomainSubset extends TransformationCriterionDomainSubset<NumberTransformationDomainSubset, NumberDomain> 
		implements NumberDomainSubset<NumberTransformationDomainSubset, NumberDomain>
{
	private static final long serialVersionUID = 1L;

	public NumberTransformationDomainSubset(VTLAlias alias, NumberDomain parent, Transformation expression)
	{
		super(alias, parent, expression);
		
		expression.getMetadata(new TransformationCriterionScope(alias, NUMBERDS));
	}

	@Override
	protected ScalarValue<?, ?, NumberTransformationDomainSubset, NumberDomain> castCasted(
		ScalarValue<?, ?, NumberTransformationDomainSubset, NumberDomain> casted)
	{
		return createNumberValue((Number) casted.get(), this);
	}
}
