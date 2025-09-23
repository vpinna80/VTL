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

import static it.bancaditalia.oss.vtl.model.data.VTLAlias.needsQuotes;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;

/**
 * A class to describe a VTL alias with respect to quoting and
 * case-insensitiveness behavior
 */
public class VTLAliasImpl implements VTLAlias, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Pattern SDMX_PATTERN = Pattern.compile(
		"^(?:(?<agency>[A-Za-z_][A-Za-z0-9_.]*):)?(?<id>[A-Za-z_][A-Za-z0-9_.]*)(?:\\((?<version>[0-9._+*~]+)\\))?(?::(?<query>(?:\\.|[A-Za-z_][A-Za-z0-9_]*)+))?$"
	);

	public static VTLAlias of(String alias)
	{
		if (alias == null)
			return null;
		else
			return VTLAliasImpl.of(alias.startsWith("'") && alias.endsWith("'"), alias);
	}

	public static VTLAlias of(boolean isQuoted, String alias)
	{
		if (alias == null)
			return null;
		
		String name = isQuoted && alias.startsWith("'") && alias.endsWith("'") ? alias.substring(1, alias.length() - 1) : alias;
		Matcher matcher = SDMX_PATTERN.matcher(name); 
		if (matcher.matches())
		{
			String id = matcher.group("id");
			String agency = matcher.group("agency");
			String version = matcher.group("version");
			String component = matcher.group("query");
			
			if (component != null && (id != null || agency != null || version != null))
				return new SDMXComponentAlias(new SDMXAlias(agency, id, version), component);
			else if (id != null && (agency != null || version != null))
				return new SDMXAlias(agency, id, version);
			else
				return new VTLAliasImpl(alias, isQuoted);
		}
		else
			return new VTLAliasImpl(alias, isQuoted);
	}

	private final String name;
	private final int hash;
	private final boolean isQuoted;

	private VTLAliasImpl(String alias, boolean isQuoted)
	{
		this.isQuoted = isQuoted;
		this.name = isQuoted && alias.startsWith("'") && alias.endsWith("'") ? alias.substring(1, alias.length() - 1) : alias;
		this.hash = this.name.toLowerCase().hashCode();
		
		if (!alias.isEmpty() && !isQuoted && needsQuotes(alias))
			throw new InvalidParameterException("Trying to create an unquoted illegal alias: " + alias);
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
	public int compareTo(VTLAlias o)
	{
		return o.isComposed() ? -o.compareTo(this) : name.compareTo(o.getName());
	}

	public boolean isQuoted()
	{
		return isQuoted;
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

		if (obj instanceof SDMXAlias)
			return name.equals(((SDMXAlias) obj).getId().getName());
		else if (obj instanceof SDMXComponentAlias)
			return name.equals(((SDMXComponentAlias) obj).getComponent().getName());
		else if (obj instanceof VTLAliasImpl)
		{
			VTLAliasImpl other = (VTLAliasImpl) obj;
			return isQuoted && other.isQuoted ? name.equals(other.name) : name.equalsIgnoreCase(other.name);
		}
		else if (obj instanceof MembershipAlias)
			return name.equals(((MembershipAlias) obj).getMemberAlias().getName());
		else
			return false;
	}
}
