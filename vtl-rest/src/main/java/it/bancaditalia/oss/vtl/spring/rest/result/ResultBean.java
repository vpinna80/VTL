package it.bancaditalia.oss.vtl.spring.rest.result;

import java.io.Serializable;

public abstract class ResultBean implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final String type;
	
	public ResultBean(String type)
	{
		this.type = type;
	}

	public String getType()
	{
		return type;
	}
}
