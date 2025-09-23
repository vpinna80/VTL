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
package it.bancaditalia.oss.vtl.impl.types.names;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class SDMXComponentAlias implements VTLAlias, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final SDMXAlias maintainable;
	private final VTLAlias component;
	private final String name;
	private final int hash;
	
	public SDMXComponentAlias(SDMXAlias structure, String component)
	{
		this.maintainable = structure;
		this.component = VTLAliasImpl.of(component);
		this.hash = component.toLowerCase().hashCode();
		this.name = structure + ":" + component;
	}

	public SDMXComponentAlias(String agency, String id, String version, String component)
	{
		this(new SDMXAlias(agency, id, version), component);
	}

	@Override
	public int compareTo(VTLAlias o)
	{
		if (o.isComposed())
			return -o.compareTo(this);
		
		int compare = component.compareTo(o);
		if (o instanceof SDMXComponentAlias && compare == 0)
			return maintainable.compareTo(o); 
		else
			return compare;
	}

	@Override
	public String getName()
	{
		return component.getName();
	}

	@Override
	public boolean isComposed()
	{
		return false;
	}

	@Override
	public VTLAlias in(VTLAlias dataset)
	{
		return new MembershipAlias(dataset, this);
	}
	
	@Override
	public int hashCode()
	{
		return hash;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		
		if (obj instanceof SDMXComponentAlias)
		{
			SDMXComponentAlias other = (SDMXComponentAlias) obj;
			return maintainable.equals(other.maintainable) && component.equals(other.component);
		}
		else if (obj instanceof VTLAlias)
		{
			VTLAlias other = (VTLAlias) obj;
			if (other.isComposed())
				other = other.getMemberAlias();
			return component.equals(other);
		}
		else
			return false;
	}

	@Override
	public String toString()
	{
		return name;
	}
	
	public SDMXAlias getMaintainable()
	{
		return maintainable;
	}
	
	public VTLAlias getComponent()
	{
		return component;
	}
}
