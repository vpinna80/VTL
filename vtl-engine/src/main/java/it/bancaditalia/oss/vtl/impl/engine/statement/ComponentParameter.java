/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.engine.statement;

import it.bancaditalia.oss.vtl.model.data.ComponentRole;

class ComponentParameter<T extends ComponentRole> extends ScalarParameter
{
	private static final long serialVersionUID = 1L;

	private final Class<T> role;

	public ComponentParameter(String name, String domainName, Class<T> role)
	{
		super(name, domainName);
		this.role = role;
	}

	public Class<T> getRole()
	{
		return role;
	}

	@Override
	public String toString()
	{
		return getName() + " " + role.getSimpleName().toLowerCase() + (domainName != null ? "<" + domainName + ">" : "");
	}
}
