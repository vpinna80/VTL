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

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public enum CommonComponents
{
	TIME_PERIOD("TIME_PERIOD", Identifier.class, TIMEDS),
	RULEID("ruleid", Measure.class, STRINGDS), 
	ERRORCODE("errorcode", Measure.class, STRINGDS), 
	ERRORLEVEL("errorlevel", Measure.class, INTEGERDS), 
	IMBALANCE("imbalance", Measure.class, NUMBERDS);
	
	private static class CommonComponent<R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements DataStructureComponent<R, S, D>, Variable<S, D>
	{
		private static final long serialVersionUID = 1L;
		
		private final String name;
		private final Class<? extends Component> role;
		private final ValueDomainSubset<?, ?> domain;

		private CommonComponent(String name, Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
		{
			this.name = name;
			this.role = role;
			this.domain = domain;
		}

		@Override
		public Variable<S, D> getVariable()
		{
			return this;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Class<R> getRole()
		{
			return (Class<R>) role;
		}

		@Override
		public String getName()
		{
			return name;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <R1 extends Component> DataStructureComponent<R1, S, D> as(Class<R1> role)
		{
			return this.role == role ? (DataStructureComponent<R1, S, D>) this : new CommonComponent<>(name, role, domain);
		}

		@SuppressWarnings("unchecked")
		@Override
		public S getDomain()
		{
			return (S) domain;
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

			if (obj instanceof CommonComponent)
			{
				CommonComponent<?, ?, ?> other = (CommonComponent<?, ?, ?>) obj;
				return role == other.role && name.equals(other.name) && domain.equals(other.domain);
			}
			else if (obj instanceof DataStructureComponent)
				return role == ((DataStructureComponent<?, ?, ?>) obj).getRole() && ((DataStructureComponent<?, ?, ?>) obj).getVariable().equals(this);
			else if (obj instanceof Variable)
				return name.equals(((Variable<?, ?>) obj).getName()) && domain.equals(((Variable<?, ?>) obj).getDomain());

			return false;
		}

		@Override
		public String toString()
		{
			return (is(Identifier.class) ? "$" : "") + (is(Attribute.class) ? "@" : "") + getVariable().getName() + "[" + getVariable().getDomain() + "]";
		}
	}

	private String vname;
	private Class<? extends Component> role;
	private ValueDomainSubset<?, ?> domain;

	private CommonComponents(String vname, Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
	{
		this.vname = vname;
		this.role = role;
		this.domain = domain;
	}
	
	public <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataStructureComponent<R, S, D> get()
	{
		return new CommonComponent<>(vname, role, domain);
	}
}
