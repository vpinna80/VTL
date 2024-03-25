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

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class DataStructureComponentImpl<R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements DataStructureComponent<R, S, D>
{
	private static final long serialVersionUID = 1L;
	private final Variable<S, D> variable;
	private final Class<R> role;
	private transient int hashCode;
	
	public DataStructureComponentImpl(Class<R> role, Variable<S, D> variable)
	{
		this.role = role;
		this.variable = variable;
		this.hashCode = hashCodeInit();
	}
	
	@Override
	public Variable<S, D> getVariable()
	{
		return variable;
	}

	@Override
	public Class<R> getRole()
	{
		return role;
	}

	@Override
	public int hashCode()
	{
		return hashCode == 0 ? hashCode = hashCodeInit() : hashCode;
	}

	public int hashCodeInit()
	{
		final int prime = 31;
		int result = 1;
		result = prime * variable.hashCode();
		result = prime * result + role.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (obj instanceof DataStructureComponent)
		{
			DataStructureComponent<?, ?, ?> other = (DataStructureComponent<?, ?, ?>) obj;
			if (role == other.getRole() && variable.equals(other.getVariable()))
				return true;
		}
		
		return false;
	}

	@Override
	public String toString()
	{
		return (is(Identifier.class) ? "$" : "") + (is(Attribute.class) ? "@" : "") + getVariable().getName() + "[" + getVariable().getDomain() + "]";	
	}

	@Override
	public DataStructureComponent<R, S, D> getRenamed(String newName)
	{
		return variable.getRenamed(newName).as(role);
	}
}
