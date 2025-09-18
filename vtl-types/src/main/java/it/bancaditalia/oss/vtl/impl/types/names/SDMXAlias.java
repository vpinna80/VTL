package it.bancaditalia.oss.vtl.impl.types.names;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class SDMXAlias implements VTLAlias, Serializable
{
	private static final long serialVersionUID = 1L;

	private final String agency; 
	private final VTLAlias id; 
	private final String version; 
	private final int hash;
	private final String name;
	
	public SDMXAlias(String agency, String id, String version)
	{
		this.agency = agency;
		this.id = VTLAliasImpl.of(true, id);
		this.version = version;
		this.hash = id.toLowerCase().hashCode();
		this.name = String.format("%s:%s(%s)", agency, id, version);
	}
	
	@Override
	public int compareTo(VTLAlias o)
	{
		if (o.isComposed())
			return -o.compareTo(this);
		
		int compare = id.compareTo(o);
		if (o instanceof SDMXAlias && compare == 0)
		{
			SDMXAlias other = (SDMXAlias) o;
			if (agency != null && other.agency != null)
				compare = agency.compareTo(other.agency);
			if (compare != 0)
				return compare;
			return version != null && other.version != null ? version.compareTo(other.agency) : 0; 
		}
		else
			return compare;
	}

	@Override
	public String getName()
	{
		return id.getName();
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
		
		if (obj instanceof SDMXAlias)
		{
			SDMXAlias other = (SDMXAlias) obj;
			
			boolean equals = true;
			if (agency != null && other.agency != null && !agency.equals(other.agency))
				equals = false;
			if (equals && version != null && other.version != null && !version.equals(other.version))
				equals = false;
			return equals && id.equals(other.id);
		}
		else if (obj instanceof VTLAlias)
		{
			VTLAlias other = (VTLAlias) obj;
			if (other.isComposed())
				other = other.getMemberAlias();
			return id.equals(other);
		}
		else
			return false;
	}
	
	@Override
	public String toString()
	{
		return name;
	}

	public String getAgency()
	{
		return agency;
	}

	public VTLAlias getId()
	{
		return id;
	}

	public String getVersion()
	{
		return version;
	}
}
