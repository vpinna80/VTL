package it.bancaditalia.oss.vtl.impl.transform.exceptions;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

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

	public VTLIncompatibleStructuresException(String description, Collection<Collection<? extends DataStructureComponent<?, ?, ?>>> structures)
	{
		super(description + ": " + structures.stream()
			.map(str -> str.stream().sorted(DataStructureComponent::byNameAndRole).collect(toList()).toString())
			.map(s -> "                    " + s).collect(joining("\n", "\n", "")));
	}
}
