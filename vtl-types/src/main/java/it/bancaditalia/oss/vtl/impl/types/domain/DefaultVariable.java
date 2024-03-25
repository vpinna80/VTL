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

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class DefaultVariable<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>
{
	private static final long serialVersionUID = 1L;

	private class DefaultVariableComponent<R extends Component> implements DataStructureComponent<R, S, D>
	{
		private static final long serialVersionUID = 1L;
		
		private final Class<R> role;

		private DefaultVariableComponent(Class<R> role)
		{
			this.role = role;
		}

		@Override
		public Variable<S, D> getVariable()
		{
			return DefaultVariable.this;
		}

		@Override
		public Class<R> getRole()
		{
			return role;
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + getEnclosingInstance().hashCode();
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
			if (obj instanceof DataStructureComponent && role == ((DataStructureComponent<?, ?, ?>) obj).getRole() && getEnclosingInstance().equals(((DataStructureComponent<?, ?, ?>) obj).getVariable()))
					return true;

			return false;
		}

		private DefaultVariable<S, D> getEnclosingInstance()
		{
			return DefaultVariable.this;
		}
		
		
	}

	private final String name;
	private final S domain;
	
	public DefaultVariable(S domain)
	{
		this.name = domain + "_var";
		this.domain = domain;
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public <R1 extends Component> DataStructureComponent<R1, S, D> as(Class<R1> role)
	{
		return new DefaultVariableComponent<R1>(role);
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
		if (obj instanceof Variable)
		{
			Variable<?, ?> inst = (Variable<?, ?>) obj;
			return name.equals(inst.getName()) && domain.equals(inst.getDomain());
		}
		else 
			return false;
	}

	@Override
	public Variable<S, D> getRenamed(String newName)
	{
		// Default variables cannot be renamed
		return ConfigurationManager.getDefault().getMetadataRepository().getVariable(newName, domain);
	}
}
