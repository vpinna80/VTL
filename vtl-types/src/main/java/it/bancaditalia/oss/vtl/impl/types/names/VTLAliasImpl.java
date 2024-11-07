package it.bancaditalia.oss.vtl.impl.types.names;

import static java.util.Objects.requireNonNull;

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

	private final String name;
	private final String quotedName;
	private final int hash;
	private final boolean isQuoted;

	public VTLAliasImpl(boolean isQuoted, String name)
	{
		this.isQuoted = isQuoted;
		quotedName = name;
		this.name = name;
		hash = this.name.toLowerCase().hashCode();
	}
	
	private VTLAliasImpl(String name)
	{
		quotedName = name;
		isQuoted = requireNonNull(name).matches("^'.*'$");
		this.name = isQuoted ? name.substring(1, name.length() - 1) : name;
		hash = this.name.toLowerCase().hashCode();
		
		if (!isQuoted && name.contains("#"))
			throw new InvalidParameterException(name);
	}
	
	public static VTLAlias of(String name)
	{
		return name != null ? new VTLAliasImpl(name) : null;
	}

	public static VTLAlias of(boolean isQuoted, String name)
	{
		return name != null ? new VTLAliasImpl(isQuoted, name) : null;
	}

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
		return isQuoted ? quotedName : name;
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
