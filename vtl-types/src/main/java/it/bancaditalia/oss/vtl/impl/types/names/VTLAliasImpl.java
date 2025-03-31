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
import java.security.InvalidParameterException;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;

/**
 * A class to describe a VTL alias with respect to quoting and
 * case-insensitiveness behavior
 */
public class VTLAliasImpl implements VTLAlias, Serializable
{
	private static final long serialVersionUID = 1L;

	public static VTLAlias of(String name)
	{
		return name != null ? new VTLAliasImpl(name, false) : null;
	}

	public static VTLAlias of(boolean isQuoted, String name)
	{
		return name != null ? new VTLAliasImpl(name, isQuoted) : null;
	}

	private final String name;
	private final int hash;
	private final boolean isQuoted;

	private VTLAliasImpl(String name, boolean isQuoted)
	{
		this.isQuoted = isQuoted;
		this.name = isQuoted && name.startsWith("'") && name.endsWith("'") ? name.substring(1, name.length() - 1) : name;
		this.hash = this.name.toLowerCase().hashCode();
		
		if (!isQuoted && !this.name.matches("^[a-zA-Z0-9_]+$"))
			throw new InvalidParameterException("Trying to create an unquoted alias with illegal characters: " + this.name);
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public boolean isComposed()
	{
		return false;
	}

	@Override
	public VTLAlias in(VTLAlias dataset)
	{
		if (dataset.isComposed())
			throw new InvalidParameterException("The alias is already composed.");
		else
			return new MembershipAlias(dataset, this);
	}

	@Override
	public String toString()
	{
		return isQuoted ? "'" + name + "'" : name;
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

		VTLAliasImpl other = (VTLAliasImpl) obj;
		if (isQuoted && other.isQuoted)
			return name.equals(other.name);
		else if (isQuoted)
			return other.name.equalsIgnoreCase(name);
		else // if (other.isQuoted)
			return name.equalsIgnoreCase(other.name);
	}
}
