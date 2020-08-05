package it.bancaditalia.oss.vtl.spring.rest.exception;

import java.util.UUID;

import it.bancaditalia.oss.vtl.exceptions.VTLException;

public class VTLInvalidSessionException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLInvalidSessionException(UUID uuid)
	{
		super("Invalid session uuid: " + uuid);
	}
}
