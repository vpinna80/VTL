package it.bancaditalia.oss.vtl.spring.rest.result;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class ComponentBean extends DomainBean implements Serializable 
{
	private static final long serialVersionUID = 1L;
	private final String name;
	private final String role;
	
	public ComponentBean(DataStructureComponent<?, ?, ?> component)
	{
		super(component.getDomain());
		
		name = component.getName();
		role = component.getRole().getSimpleName();
	}

	public String getName()
	{
		return name;
	}

	public String getRole()
	{
		return role;
	}
}