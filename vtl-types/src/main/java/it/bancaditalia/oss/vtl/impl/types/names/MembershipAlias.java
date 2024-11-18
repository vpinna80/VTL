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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.Objects;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class MembershipAlias implements VTLAlias, Serializable
{
	private static final long serialVersionUID = 1L;

	private final VTLAlias member;
	private final VTLAlias dsName;
	private final int hash;

	public MembershipAlias(VTLAlias dsName, VTLAlias member)
	{
		this.dsName = requireNonNull(dsName);
		this.member = requireNonNull(member);
		hash = Objects.hash(dsName, member);
	}

	@Override
	public String getName()
	{
		return dsName.getName() + "#" + member.getName();
	}

	@Override
	public String toString()
	{
		return dsName + "#" + member;
	}
	
	@Override
	public boolean isComposed()
	{
		return true;
	}
	
	@Override
	public VTLAlias in(VTLAlias dataset)
	{
		throw new InvalidParameterException("The alias is already composed.");
	}
	
	@Override
	public VTLAlias getMemberAlias()
	{
		return member;
	}
	
	@Override
	public Entry<VTLAlias, VTLAlias> split()
	{
		return new SimpleEntry<>(dsName, member);
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
		if (getClass() != obj.getClass())
			return false;

		MembershipAlias other = (MembershipAlias) obj;
		return dsName.equals(other.dsName) && member.equals(other.member);
	}
}