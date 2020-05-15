package it.bancaditalia.oss.vtl.impl.transform.exceptions;

import static java.util.stream.Collectors.joining;

import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class VTLAmbiguousComponentException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLAmbiguousComponentException(String name, Set<DataStructureComponent<?, ?, ?>> ambiguousSet)
	{
		super(ambiguousSet.stream().map(Object::toString).collect(joining(", ", "Component " + name + " is ambiguous: it appears as ", ".")));
	}
}
