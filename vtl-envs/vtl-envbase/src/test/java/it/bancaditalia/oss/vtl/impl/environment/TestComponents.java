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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public enum TestComponents
{
	STR_ID("STR_ID", Identifier.class, STRINGDS),
	INT_ID("INT_ID", Identifier.class, INTEGERDS),
	INT_ME("INT_ME", Measure.class, INTEGERDS),
	BOL_ME("BOL_ME", Measure.class, BOOLEANDS),
	IDENTIFIER("IDENTIFIER", Identifier.class, DATEDS),
	MEASURE("MEASURE", Measure.class, NUMBERDS),
	ATTRIBUTE("ATTRIBUTE", Attribute.class, STRINGDS),
	QUOTED("QUOTED", Attribute.class, STRINGDS);
	
	private String vname;
	private Class<? extends Component> role;
	private ValueDomainSubset<?, ?> domain;

	private TestComponents(String vname, Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
	{
		this.vname = vname;
		this.role = role;
		this.domain = domain;
	}
	
	@SuppressWarnings("unchecked")
	public <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataStructureComponent<R, S, D> get()
	{
		return new TestComponent<R, S, D>(vname, (Class<R>) role, (S) domain);
	}
}
