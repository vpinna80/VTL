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
package it.bancaditalia.oss.vtl.impl.transform.testutils;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class TestComponent<R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements DataStructureComponent<R, S, D>
{
	private static final long serialVersionUID = 1L;

	private final VTLAlias alias;
	private final Class<R> role;
	private final S domain;

	private class VariableView implements Variable<S, D>
	{
		private static final long serialVersionUID = 1L;
		
		@Override
		public VTLAlias getAlias()
		{
			return alias;
		}

		@Override
		public <R1 extends Component> DataStructureComponent<R1, S, D> as(Class<R1> role)
		{
			return new TestComponent<>(alias, role, domain);
		}

		@Override
		public S getDomain()
		{
			return domain;
		}

		@Override
		public int hashCode()
		{
			return varHash();
		}
		
		@Override
		public boolean equals(Object obj)
		{
			return obj instanceof Variable ? alias.equals(((Variable<?, ?>) obj).getAlias()) && domain.equals(((Variable<?, ?>) obj).getDomain()) : false;
		}
	}

	public TestComponent(VTLAlias alias, Class<R> role, S domain)
	{
		this.alias = alias;
		this.role = role;
		this.domain = domain;
	}

	@Override
	public Variable<S, D> getVariable()
	{
		return new VariableView();
	}

	@Override
	public Class<R> getRole()
	{
		return role;
	}

	private int varHash()
	{
		int prime = 31;
		int result = 1;
		result = prime * result + domain.hashCode();
		result = prime * result + alias.hashCode();
		return result;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * varHash();
		result = prime * result + role.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;

		if (obj instanceof TestComponent)
		{
			TestComponent<?, ?, ?> other = (TestComponent<?, ?, ?>) obj;
			return role == other.role && alias.equals(other.alias) && domain.equals(other.domain);
		}
		else if (obj instanceof DataStructureComponent)
		{
			DataStructureComponent<?, ?, ?> other = (DataStructureComponent<?, ?, ?>) obj;
			return role == other.getRole() && alias.equals(other.getVariable().getAlias()) && domain.equals(other.getVariable().getDomain());
		}
		else if (obj instanceof Variable)
			return alias.equals(((Variable<?, ?>) obj).getAlias()) && domain.equals(((Variable<?, ?>) obj).getDomain());

		return false;
	}
	
	@Override
	public String toString()
	{
		return (is(Identifier.class) ? "$" : "") + (is(Attribute.class) ? "@" : "") + getVariable().getAlias() + "[" + getVariable().getDomain() + "]";	
	}
}