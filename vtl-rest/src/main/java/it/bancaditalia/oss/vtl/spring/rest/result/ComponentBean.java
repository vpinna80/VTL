package it.bancaditalia.oss.vtl.spring.rest.result;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class ComponentBean implements Serializable 
{
	private static final long serialVersionUID = 1L;
	private final String name;
	private final String role;
	private final String domain;
	
	public ComponentBean(DataStructureComponent<?, ?, ?> component)
	{
		name = component.getName();
		role = component.getRole().getSimpleName();
		domain = component.getDomain().toString();
	}

	public String getName()
	{
		return name;
	}

	public String getRole()
	{
		return role;
	}

	public String getDomain()
	{
		return domain;
	}
}