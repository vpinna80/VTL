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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;

import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class IntegerTransformationDomainSubset extends TransformationCriterionDomainSubset<IntegerTransformationDomainSubset, IntegerDomain> 
		implements IntegerDomainSubset<IntegerTransformationDomainSubset>
{
	private static final long serialVersionUID = 1L;

	public IntegerTransformationDomainSubset(VTLAlias alias, IntegerDomain parent, Transformation expression)
	{
		super(alias, parent, expression);
		
		expression.getMetadata(new TransformationCriterionScope(alias, INTEGERDS));
	}

	@Override
	protected ScalarValue<?, ?, IntegerTransformationDomainSubset, IntegerDomain> castCasted(
		ScalarValue<?, ?, IntegerTransformationDomainSubset, IntegerDomain> casted)
	{
		return IntegerValue.of((Long) casted.get(), this);
	}
}
