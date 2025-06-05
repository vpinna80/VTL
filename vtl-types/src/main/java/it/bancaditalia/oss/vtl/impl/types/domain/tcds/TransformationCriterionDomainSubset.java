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

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.domain.CriterionDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.DefaultVariable;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public abstract class TransformationCriterionDomainSubset<S extends TransformationCriterionDomainSubset<S, D>, D extends ValueDomain> extends CriterionDomainSubset<S, D>
{
	public static final VTLAlias TEST_ALIAS = VTLAliasImpl.of("'#TEST_ALIAS#'");
	
	private static final long serialVersionUID = 1L;

	private final Transformation transformation;

	public TransformationCriterionDomainSubset(VTLAlias alias, D parent, Transformation expression)
	{
		super(alias, parent);

		this.transformation = expression;
	}

	@Override
	public Transformation getCriterion()
	{
		return transformation;
	}

	@Override
	public boolean test(ScalarValue<?, ?, S, D> value)
	{
		VTLValue result = transformation.eval(new TransformationCriterionScope<>(value));
		return result instanceof BooleanValue && result == BooleanValue.TRUE; 
	}

	@Override
	public Variable<S, D> getDefaultVariable()
	{
		@SuppressWarnings("unchecked")
		S domain = (S) this;
		return new DefaultVariable<>(domain);
	}
}
