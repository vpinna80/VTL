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

import it.bancaditalia.oss.vtl.impl.types.domain.NonNullDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.StrlenDomainSubset;
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
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public enum TestComponents
{
	BREAKS(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(350))),
	COLLECTION(Attribute.class, "ECB:CL_COLLECTION(1.0)"),
	COMPILATION(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	COMPILING_ORG(Attribute.class, "ECB:CL_ORGANISATION(1.0)"),
	COVERAGE(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(350))),
	CURRENCY(Identifier.class, "ECB:CL_CURRENCY(1.0)"),
	CURRENCY_DENOM(Identifier.class, "ECB:CL_CURRENCY(1.0)"),
	DECIMALS(Attribute.class, "ECB:CL_DECIMALS(1.0)"),
	DISS_ORG(Attribute.class, "ECB:CL_ORGANISATION(1.0)"),
	DOM_SER_IDS(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(70))),
	EXR_SUFFIX(Identifier.class, "ECB:CL_EXR_SUFFIX(1.0)"),
	EXR_TYPE(Identifier.class, "ECB:CL_EXR_TYPE(1.0)"),
	FREQ(Identifier.class, "ECB:CL_FREQ(1.0)"),
	NAT_TITLE(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(350))),
	OBS_COM(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	OBS_CONF(Attribute.class, "ECB:CL_OBS_CONF(1.0)"),
	OBS_PRE_BREAK(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(30))),
	OBS_STATUS(Attribute.class, "ECB:CL_OBS_STATUS(1.0)"),
	OBS_VALUE(Measure.class, NUMBERDS),
	PUBL_ECB(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	PUBL_PUBLIC(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	PUBL_MU(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(1050))),
	SOURCE_AGENCY(Attribute.class, "ECB:CL_ORGANISATION(1.0)"),
	SOURCE_PUB(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(350))),
	TITLE(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(200))),
	TITLE_COMPL(Attribute.class, new NonNullDomainSubset<>(new StrlenDomainSubset(STRINGDS, OptionalInt.of(1), OptionalInt.of(1050)))),
	TIME_FORMAT(Attribute.class, new NonNullDomainSubset<>(new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(3)))),
	UNIT(Attribute.class, "ECB:CL_UNIT(1.0)"),
	UNIT_INDEX_BASE(Attribute.class, new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(35))),
	UNIT_MULT(Attribute.class, "ECB:CL_UNIT_MULT(1.0)");

	private static class TestVariable<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>
	{
		private static final long serialVersionUID = 1L;

		private class TestComponent<R extends Component> implements DataStructureComponent<R, S, D>
		{
			private static final long serialVersionUID = 1L;
			
			private final Class<R> role;

			private TestComponent(Class<R> role)
			{
				this.role = role;
			}

			@Override
			public Variable<S, D> getVariable()
			{
				return TestVariable.this;
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
					return role == other.getRole() && TestVariable.this.equals(other.getVariable());
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
		
		public TestVariable(VTLAlias name, S domain)
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
			return new TestComponent<>(role);
		}
		
		@Override
		public int hashCode()
		{
			return defaultHashCode();
		}
	}

	private final Class<? extends Component> role;
	private final ValueDomainSubset<?, ?> domain;
	private final VTLAlias domainStr;

	private TestComponents(Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
	{
		this.role = role;
		this.domain = domain;
		this.domainStr = null;
	}
	
	private TestComponents(Class<? extends Component> role, String domainStr)
	{
		this.role = role;
		this.domainStr = VTLAliasImpl.of(true, domainStr);
		this.domain = null;
	}
	
	@SuppressWarnings("unchecked")
	public <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataStructureComponent<R, S, D> get(MetadataRepository repo)
	{
		return (domain != null ? new TestVariable<>(VTLAliasImpl.of(name()), (S) domain) : new TestVariable<>(VTLAliasImpl.of(name()), (S) repo.getDomain(domainStr).get())).as((Class<R>) role);
	}
}
