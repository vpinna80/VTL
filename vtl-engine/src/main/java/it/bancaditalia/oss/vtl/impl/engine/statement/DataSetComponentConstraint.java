package it.bancaditalia.oss.vtl.impl.engine.statement;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public abstract class DataSetComponentConstraint implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final String name;

	public DataSetComponentConstraint(String name)
	{
		this.name = name;
	}

	public String getName()
	{
		return name;
	}

	protected abstract Optional<Set<? extends DataStructureComponent<?, ?, ?>>> matchStructure(DataSetMetadata structure, MetadataRepository repo);
}