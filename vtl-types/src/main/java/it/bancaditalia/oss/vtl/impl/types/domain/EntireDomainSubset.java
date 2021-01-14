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
package it.bancaditalia.oss.vtl.impl.types.domain;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

public abstract class EntireDomainSubset<S extends Comparable<? super S>, P extends ValueDomain> implements ValueDomainSubset<P>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final P parentDomain;
	private final String varName; 
	
	public EntireDomainSubset(P parentDomain, String defaultVarName)
	{
		this.parentDomain = parentDomain;
		this.varName = defaultVarName;

		// check no cycles in subsetting
		ValueDomain parent = parentDomain;
		while (parent != null)
		{
			if (parent == this)
				throw new UnsupportedOperationException("New subset of domain would form a cycle with parent " + parent + ".");
			
			if (parent instanceof ValueDomainSubset)
				parent = ((ValueDomainSubset<?>) parent).getParentDomain();
		}
	}

	@Override
	public Object getCriterion()
	{
		throw new UnsupportedOperationException("getCriterion");
	}

	@Override
	public P getParentDomain()
	{
		return parentDomain;
	}
	
	@Override
	public String getVarName()
	{
		return varName;
	}
}
