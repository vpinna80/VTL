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

import static java.util.regex.Pattern.compile;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.function.Predicate;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;

/**
 * A class to describe a VTL alias with respect to quoting and
 * case-insensitiveness behavior
 */
public class VTLAliasImpl implements VTLAlias, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Predicate<String> PATTERN = compile("^[A-Za-z_][A-Za-z0-9_.]*|[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_.]*(?:\\([0-9._+*~]+\\))?(?::(?:\\.|[A-Za-z_][A-Za-z0-9_]*)+)?$").asMatchPredicate();

	public static VTLAlias of(String alias)
	{
		if (alias == null)
			return null;
		else
			return VTLAliasImpl.of(alias.startsWith("'") && alias.endsWith("'"), alias);
	}

	public static VTLAlias of(boolean isQuoted, String alias)
	{
		return alias == null ? null : new VTLAliasImpl(alias, isQuoted);
	}

	private final String name;
	private final int hash;
	private final boolean isQuoted;

	private VTLAliasImpl(String alias, boolean isQuoted)
	{
		this.isQuoted = isQuoted;
		this.name = isQuoted && alias.startsWith("'") && alias.endsWith("'") ? alias.substring(1, alias.length() - 1) : alias;
		this.hash = this.name.toLowerCase().hashCode();
		
		if (!isQuoted && !PATTERN.test(this.name))
			throw new InvalidParameterException("Trying to create an unquoted illegal alias: " + this.name);
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
		if (getClass() != obj.getClass())
			return false;

		VTLAliasImpl other = (VTLAliasImpl) obj;
		return isQuoted && other.isQuoted ? name.equals(other.name) : name.equalsIgnoreCase(other.name);
	}
}
