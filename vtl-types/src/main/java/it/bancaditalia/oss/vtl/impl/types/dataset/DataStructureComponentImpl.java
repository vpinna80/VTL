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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Variable;

public class DataStructureComponentImpl<R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> implements DataStructureComponent<R, S, D>
{
	private static final long serialVersionUID = 1L;
	private final S domain;
	private final String name;
	private final Class<R> type;
	private final transient int hashCode;
	
	public DataStructureComponentImpl(String name, Class<R> type, S domain)
	{
		this.domain = domain;
		this.type = type;
		this.name = name;
		this.hashCode = hashCodeInit();
	}

	@Override
	public Variable getVariable()
	{
		return new VariableImpl(name);
	}

	@Override
	public S getDomain()
	{
		return domain;
	}

	@Override
	public Class<R> getRole()
	{
		return type;
	}

	@Override
	public DataStructureComponent<R, S, D> rename(String newName)
	{
		return new DataStructureComponentImpl<>(newName, type, domain);
	}

	@Override
	public int hashCode()
	{
		return hashCode;
	}

	public int hashCodeInit()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((domain == null) ? 0 : domain.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		DataStructureComponentImpl<?, ?, ?> other = (DataStructureComponentImpl<?, ?, ?>) obj;
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
		if (type == null)
		{
			if (other.type != null)
				return false;
		}
		else if (!type.equals(other.type))
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return (is(Identifier.class) ? "$" : "") + (is(Attribute.class) ? "@" : "") + getVariable().getName() + "[" + getDomain() + "]";	
	}
}
