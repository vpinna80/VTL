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
package it.bancaditalia.oss.vtl.impl.types.statement;

import it.bancaditalia.oss.vtl.model.data.Component.Role;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.ComponentParameterType;

public class ComponentParameterTypeImpl extends ScalarParameterTypeImpl implements ComponentParameterType
{
	private static final long serialVersionUID = 1L;

	private final Role role;

	public ComponentParameterTypeImpl(Role role, VTLAlias domainName, ScalarValue<?, ?, ?, ?> defaultValue)
	{
		super(domainName, defaultValue);
		
		this.role = role;
	}

	public Role getRole()
	{
		return role;
	}

	@Override
	public String toString()
	{
		return role.toString().toLowerCase() + (getDomainName() != null ? "<" + getDomainName() + ">" : "");
	}
}
