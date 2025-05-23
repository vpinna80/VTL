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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;

import java.util.HashMap;
import java.util.Map;

import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class DefaultVariable<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>
{
	private static final long serialVersionUID = 1L;
	
	private static final Map<ValueDomainSubset<?, ?>, String> DOMAIN_TO_VAR = new HashMap<>();
	
	static
	{
		DOMAIN_TO_VAR.put(INTEGERDS, "int_var");
		DOMAIN_TO_VAR.put(NUMBERDS, "num_var");
		DOMAIN_TO_VAR.put(BOOLEANDS, "bool_var");
		DOMAIN_TO_VAR.put(Domains.TIME_PERIODDS, "period_var");
	}

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
			return prime * result + ((role == null) ? 0 : role.hashCode());
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
		
		@Override
		public String toString()
		{
			return (is(Identifier.class) ? "$" : "") + (is(Attribute.class) ? "@" : "") + getVariable().getAlias() + "[" + getVariable().getDomain() + "]";	
		}
	}

	private final VTLAlias alias;
	private final S domain;
	
	public DefaultVariable(S domain)
	{
		this.alias = VTLAliasImpl.of(DOMAIN_TO_VAR.getOrDefault(domain, domain + "_var"));
		this.domain = domain;
	}

	@Override
	public VTLAlias getAlias()
	{
		return alias;
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
		result = prime * result + domain.hashCode();
		result = prime * result + alias.hashCode();
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
			return alias.equals(inst.getAlias()) && domain.equals(inst.getDomain());
		}
		else 
			return false;
	}
}
