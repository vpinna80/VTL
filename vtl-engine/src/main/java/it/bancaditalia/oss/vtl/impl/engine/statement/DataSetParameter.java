package it.bancaditalia.oss.vtl.impl.engine.statement;

import static java.util.stream.Collectors.joining;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class DataSetParameter extends Parameter
{
	private static final long serialVersionUID = 1L;
	
	private final List<DataSetComponentConstraint> constraints;
	
	public DataSetParameter(String name, List<DataSetComponentConstraint> constraints)
	{
		super(name);
		
		this.constraints = constraints;
	}

	@Override
	public boolean matches(VTLValueMetadata metadata)
	{
		MetadataRepository repo = ConfigurationManager.getDefault().getMetadataRepository();
		
		if (metadata instanceof DataSetMetadata)
		{
			DataSetMetadata datasetMeta = (DataSetMetadata) metadata;
			Set<DataStructureComponent<?, ?, ?>> componentsRemaining = new HashSet<>(datasetMeta);
			
			// remove components once they are matched by each constraint
			for (DataSetComponentConstraint constraint: constraints)
			{
				Optional<Set<? extends DataStructureComponent<?, ?, ?>>> matchedComponents = constraint
						.matchStructure(new DataStructureBuilder(componentsRemaining).build(), repo);
				if (!matchedComponents.isPresent())
					return false;
				
				componentsRemaining.removeAll(matchedComponents.get());
			}
			
			// the entire structure is matched if all components have been matched
			return componentsRemaining.isEmpty();
		}
		else 
			return metadata instanceof UnknownValueMetadata;
	}

	@Override
	public String getMetaString()
	{
		return constraints.stream().map(Object::toString).collect(joining(", ", "dataset{", "}"));
	}
	
	@Override
	public String toString()
	{
		return getName() + " " + getMetaString();
	}
}
