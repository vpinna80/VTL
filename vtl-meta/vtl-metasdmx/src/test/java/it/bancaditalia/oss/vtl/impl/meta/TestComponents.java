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
package it.bancaditalia.oss.vtl.impl.meta;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.util.OptionalInt;

import it.bancaditalia.oss.vtl.impl.types.domain.StrlenDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public enum TestComponents
{
	BREAKS(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(350))),
	COLLECTION(Attribute.class, "ECB:CL_COLLECTION(1.0)"),
	COMPILATION(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	COMPILING_ORG(Attribute.class, "ECB:CL_ORGANISATION(1.0)"),
	COVERAGE(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(350))),
	CURRENCY(Identifier.class, "ECB:CL_CURRENCY(1.0)"),
	CURRENCY_DENOM(Identifier.class, "ECB:CL_CURRENCY(1.0)"),
	DECIMALS(Attribute.class, "ECB:CL_DECIMALS(1.0)"),
	DISS_ORG(Attribute.class, "ECB:CL_ORGANISATION(1.0)"),
	DOM_SER_IDS(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(70))),
	EXR_SUFFIX(Identifier.class, "ECB:CL_EXR_SUFFIX(1.0)"),
	EXR_TYPE(Identifier.class, "ECB:CL_EXR_TYPE(1.0)"),
	FREQ(Identifier.class, "ECB:CL_FREQ(1.0)"),
	NAT_TITLE(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(350))),
	OBS_COM(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	OBS_CONF(Attribute.class, "ECB:CL_OBS_CONF(1.0)"),
	OBS_PRE_BREAK(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(30))),
	OBS_STATUS(Attribute.class, "ECB:CL_OBS_STATUS(1.0)"),
	OBS_VALUE(Measure.class, NUMBERDS),
	PUBL_ECB(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	PUBL_PUBLIC(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	PUBL_MU(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	SOURCE_AGENCY(Attribute.class, "ECB:CL_ORGANISATION(1.0)"),
	SOURCE_PUB(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(350))),
	TITLE(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(200))),
	TITLE_COMPL(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.of(1), OptionalInt.of(1050))),
	TIME_FORMAT(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(3))),
	UNIT(Attribute.class, "ECB:CL_UNIT(1.0)"),
	UNIT_INDEX_BASE(Attribute.class, new StrlenDomainSubset<>(STRINGDS, OptionalInt.empty(), OptionalInt.of(35))),
	UNIT_MULT(Attribute.class, "ECB:CL_UNIT_MULT(1.0)");

	private static class TestComponent<R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements DataStructureComponent<R, S, D>, Variable<S, D>
	{
		private static final long serialVersionUID = 1L;
		
		private final String name;
		private final Class<? extends Component> role;
		private final ValueDomainSubset<?, ?> domain;

		private TestComponent(String name, Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
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
		public <R1 extends Component> DataStructureComponent<R1, S, D> getComponent(Class<R1> role)
		{
			return this.role == role ? (DataStructureComponent<R1, S, D>) this : new TestComponent<>(name, role, domain);
		}

		@SuppressWarnings("unchecked")
		@Override
		public S getDomain()
		{
			return (S) domain;
		}

		@Override
		public TestComponent<R, S, D> getRenamed(String newName)
		{
			return new TestComponent<>(newName, role, domain);
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

			if (obj instanceof TestComponent)
			{
				TestComponent<?, ?, ?> other = (TestComponent<?, ?, ?>) obj;
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

	private final Class<? extends Component> role;
	private final ValueDomainSubset<?, ?> domain;
	private final String domainStr;

	private TestComponents(Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
	{
		this.role = role;
		this.domain = domain;
		this.domainStr = null;
	}
	
	private TestComponents(Class<? extends Component> role, String domainStr)
	{
		this.role = role;
		this.domainStr = domainStr;
		this.domain = null;
	}
	
	public <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataStructureComponent<R, S, D> get(MetadataRepository repo)
	{
		return domain != null ? new TestComponent<>(name(), role, domain) : new TestComponent<>(name(), role, repo.getDomain(domainStr));
	}
}
