package it.bancaditalia.oss.vtl.impl.types.names;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class SDMXComponentAlias implements VTLAlias, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final SDMXAlias structure;
	private final VTLAlias component;
	private final String name;
	private final int hash;
	
	public SDMXComponentAlias(SDMXAlias structure, String component)
	{
		this.structure = structure;
		this.component = VTLAliasImpl.of(component);
		this.hash = component.toLowerCase().hashCode();
		this.name = String.format("%s:%s", structure, component);
	}
	
	@Override
	public int compareTo(VTLAlias o)
	{
		if (o.isComposed())
			return -o.compareTo(this);
		
		int compare = component.compareTo(o);
		if (o instanceof SDMXComponentAlias && compare == 0)
			return structure.compareTo(o); 
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
			return structure.equals(other.structure) && component.equals(other.component);
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
		return structure;
	}
	
	public VTLAlias getComponent()
	{
		return component;
	}
}
