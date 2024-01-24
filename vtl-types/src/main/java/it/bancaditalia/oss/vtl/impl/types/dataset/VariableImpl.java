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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class VariableImpl<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>, Serializable
{
	private static final long serialVersionUID = 1L;
	private final String name;
	private final S domain;
	
	public VariableImpl(String name, S domain)
	{
		this.name = name;
		this.domain = domain;
	}
	
	public VariableImpl(S domain)
	{
		this.name = domain.getDefaultVariableName();
		this.domain = domain;
	}

	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public S getDomain()
	{
		return domain;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((domain == null) ? 0 : domain.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		VariableImpl<?, ?> other = (VariableImpl<?, ?>) obj;
		if (domain == null)
		{
			if (other.domain != null)
				return false;
		}
		else if (!domain.equals(other.domain))
			return false;
		if (name == null)
		{
			if (other.name != null)
				return false;
		}
		else if (!name.equals(other.name))
			return false;
		return true;
	}
}
