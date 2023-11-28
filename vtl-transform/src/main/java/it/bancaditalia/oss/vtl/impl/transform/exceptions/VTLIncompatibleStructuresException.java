package it.bancaditalia.oss.vtl.impl.transform.exceptions;

import java.util.Collection;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class VTLIncompatibleStructuresException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLIncompatibleStructuresException(String description, Collection<? extends DataStructureComponent<?, ?, ?>> expected, Collection<? extends DataStructureComponent<?, ?, ?>> actual)
	{
		super(description + ": expected " + expected + ", but was " + actual);
	}
}
