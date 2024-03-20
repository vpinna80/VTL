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
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class ComponentMock<R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements DataStructureComponent<R, S, D>, Variable<S, D>
{
	private static final long serialVersionUID = 1L;
	
	private final Class<R> role;
	private final String name;
	private final S domain;
	
	public ComponentMock(Class<R> role, S domain)
	{
		this(domain.toString().toLowerCase() + "_var", role, domain);
	}

	public ComponentMock(String name, Class<R> role, S domain)
	{
		this.role = role;
		this.name = name;
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
	public Variable<S, D> getVariable()
	{
		return this;
	}

	@Override
	public Class<R> getRole()
	{
		return role;
	}

	@Override
	public ComponentMock<R, S, D> getRenamed(String newName)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((domain == null) ? 0 : domain.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((role == null) ? 0 : role.hashCode());
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
		ComponentMock<?, ?, ?> other = (ComponentMock<?, ?, ?>) obj;
		if (!domain.equals(other.domain))
			return false;
		if (!name.equals(other.name))
			return false;
		if (role != other.role)
			return false;
		return true;
	}

	@Override
	public <R1 extends Component> DataStructureComponent<R1, S, D> getComponent(Class<R1> role)
	{
		throw new UnsupportedOperationException();
	}
}
