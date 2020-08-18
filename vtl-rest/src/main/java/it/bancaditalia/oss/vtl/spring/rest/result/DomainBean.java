package it.bancaditalia.oss.vtl.spring.rest.result;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

public class DomainBean implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final String domain;

	public DomainBean(ValueDomainSubset<?> domain)
	{
		this.domain = domain.toString();
	}
	
	public String getDomain()
	{
		return domain;
	}
}
