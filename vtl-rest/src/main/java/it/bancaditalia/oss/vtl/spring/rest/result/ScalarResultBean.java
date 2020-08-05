package it.bancaditalia.oss.vtl.spring.rest.result;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class ScalarResultBean extends ResultBean
{
	private static final long serialVersionUID = 1L;

	private final String domain;
	private final Serializable value;

	public ScalarResultBean(ScalarValue<?, ?, ?> value)
	{
		super("SCALAR");
		
		this.value = ((ScalarValue<?, ?, ?>) value).get();
		this.domain = ((ScalarValue<?, ?, ?>) value).getDomain().toString();
	}

	public String getDomain()
	{
		return domain;
	}

	public Serializable getValue()
	{
		return value;
	}
}
