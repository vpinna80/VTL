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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public enum CommonComponents
{
	TIME_PERIOD("TIME_PERIOD", Identifier.class, TIMEDS),
	RULEID("ruleid", Identifier.class, STRINGDS), 
	ERRORCODE("errorcode", Measure.class, STRINGDS), 
	ERRORLEVEL("errorlevel", Measure.class, INTEGERDS), 
	IMBALANCE("imbalance", Measure.class, NUMBERDS);
	
	private static class CommonVariable<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>
	{
		private static final long serialVersionUID = 1L;

		private class CommonComponent<R extends Component> implements DataStructureComponent<R, S, D>
		{
			private static final long serialVersionUID = 1L;
			
			private final Class<R> role;

			private CommonComponent(Class<R> role)
			{
				this.role = role;
			}

			@Override
			public Variable<S, D> getVariable()
			{
				return CommonVariable.this;
			}

			@Override
			public Class<R> getRole()
			{
				return role;
			}

			@Override
			public int hashCode()
			{
				return defaultHashCode();
			}

			@Override
			public boolean equals(Object obj)
			{
				if (this == obj)
					return true;

				if (obj instanceof DataStructureComponent)
				{
					DataStructureComponent<?, ?, ?> other = (DataStructureComponent<?, ?, ?>) obj;
					return role == other.getRole() && CommonVariable.this.equals(other.getVariable());
				}
				
				return false;
			}

			@Override
			public String toString()
			{
				return (is(Identifier.class) ? "$" : "") + (is(Attribute.class) ? "@" : "") + getVariable().getAlias() + "[" + getVariable().getDomain() + "]";
			}
		}

		private final VTLAlias name;
		private final S domain;
		
		public CommonVariable(VTLAlias name, S domain)
		{
			this.name = name;
			this.domain = domain;
		}
		
		public VTLAlias getAlias()
		{
			return name;
		}

		public S getDomain()
		{
			return domain;
		}
	
		@Override
		public <R1 extends Component> DataStructureComponent<R1, S, D> as(Class<R1> role)
		{
			return new CommonComponent<>(role);
		}
		
		@Override
		public int hashCode()
		{
			return defaultHashCode();
		}
		
		@Override
		public boolean equals(Object obj)
		{
			if (obj instanceof Variable)
			{
				Variable<?, ?> other = (Variable<?, ?>) obj;
				return name.equals(other.getAlias()) && domain.equals(other.getDomain());
			}
			
			return false;
		}
	}

	private VTLAlias vname;
	private Class<? extends Component> role;
	private ValueDomainSubset<?, ?> domain;

	private CommonComponents(String vname, Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
	{
		this.vname = VTLAliasImpl.of(vname);
		this.role = role;
		this.domain = domain;
	}
	
	private CommonComponents(VTLAlias vname, Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
	{
		this.vname = vname;
		this.role = role;
		this.domain = domain;
	}
	
	@SuppressWarnings("unchecked")
	public <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataStructureComponent<R, S, D> get()
	{
		return new CommonVariable<>(vname, (S) domain).as((Class<R>) role);
	}
}
