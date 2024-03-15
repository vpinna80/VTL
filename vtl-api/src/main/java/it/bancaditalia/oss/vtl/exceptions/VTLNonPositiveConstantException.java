package it.bancaditalia.oss.vtl.exceptions;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class VTLNonPositiveConstantException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLNonPositiveConstantException(String operation, ScalarValue<?, ?, ?, ?> constant)
	{
		super("In " + operation + ", constant " + constant + " is not strictly positive.");
	}
}
