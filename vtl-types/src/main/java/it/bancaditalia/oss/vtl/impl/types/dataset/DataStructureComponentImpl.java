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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class DataStructureComponentImpl<R extends Component> implements DataStructureComponent<R>
{
	private static final long serialVersionUID = 1L;

	private final VTLAlias alias;
	private final Class<R> role;
	private final int hashCode;

	public DataStructureComponentImpl(VTLAlias alias, Class<R> role)
	{
		this.alias = requireNonNull(alias);
		this.role = requireNonNull(role);
		this.hashCode = Objects.hash(alias, role);
	}

	@Override
	public VTLAlias getAlias()
	{
		return alias;
	}

	@Override
	public Class<R> getRole()
	{
		return role;
	}

	@Override
	public int hashCode()
	{
		return hashCode;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof DataStructureComponent))
			return false;
		
		DataStructureComponent<?> other = (DataStructureComponent<?>) obj;
		return role == other.getRole() && alias.equals(other.getAlias());
	}
	
	@Override
	public String toString()
	{
		return String.format("%s%s", role == Identifier.class ? "$" : role == Attribute.class 
			? "#" : role == ViralAttribute.class ? "##" : "", alias);
	}
}
