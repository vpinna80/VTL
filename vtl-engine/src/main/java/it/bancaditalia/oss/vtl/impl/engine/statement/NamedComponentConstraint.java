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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class NamedComponentConstraint extends DataSetComponentConstraint
{
	private static final long serialVersionUID = 1L;

	private final Class<? extends ComponentRole> role;
	private final String domainName;
	
	public NamedComponentConstraint(String name, Class<? extends ComponentRole> role, String domainName)
	{
		super(name);

		this.role = role;
		this.domainName = domainName;
	}
	
	@Override
	protected Optional<Set<? extends DataStructureComponent<?, ?, ?>>> matchStructure(DataSetMetadata structure, MetadataRepository repo)
	{
		Optional<? extends DataStructureComponent<?, ?, ?>> component = structure.getComponent(getName(), role);
		if (domainName != null)
			component = component.filter(c -> repo.getDomain(domainName).isAssignableFrom(c.getDomain()));

		return component.map(Collections::singleton);
	}
	
	@Override
	public String toString()
	{
		return getName() + " " + role.getSimpleName().toLowerCase() 
				+ (domainName != null ? "<" + domainName + ">" : "");
	}
}
