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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class StringTransformationDomainSubset<V extends StringValue<?, S>, S extends StringDomainSubset<S>> 
		extends TransformationCriterionDomainSubset<StringTransformationDomainSubset<V, S>, V, S, StringDomain> 
		implements StringDomainSubset<StringTransformationDomainSubset<V, S>>
{
	private static final long serialVersionUID = 1L;

	public StringTransformationDomainSubset(VTLAlias alias, S parent, Transformation expression)
	{
		super(alias, parent, expression);
		
		expression.getMetadata(new TransformationCriterionScope<>(STRINGDS));
	}

	@Override
	protected ScalarValue<?, ?, StringTransformationDomainSubset<V, S>, StringDomain> castCasted(V casted)
	{
		return StringValue.of(casted.get(), this);
	}
}
