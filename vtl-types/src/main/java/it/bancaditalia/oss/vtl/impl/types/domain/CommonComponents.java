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

import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public enum CommonComponents
{
	TIME_PERIOD("TIME_PERIOD", Identifier.class, TIMEDS),
	RULEID("ruleid", Identifier.class, STRINGDS), 
	ERRORCODE("errorcode", Measure.class, STRINGDS), 
	ERRORLEVEL("errorlevel", Measure.class, INTEGERDS), 
	IMBALANCE("imbalance", Measure.class, NUMBERDS);

	private VTLAlias alias;
	private Class<? extends Component> role;
	private ValueDomainSubset<?, ?> domain;

	private CommonComponents(String vname, Class<? extends Component> role, ValueDomainSubset<?, ?> domain)
	{
		this.alias = VTLAliasImpl.of(vname);
		this.role = role;
		this.domain = domain;
	}
	
	@SuppressWarnings("unchecked")
	public <R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> DataSetComponent<R, S, D> get()
	{
		return (DataSetComponent<R, S, D>) DataSetComponentImpl.of(alias, domain, role);
	}
}
