package it.bancaditalia.oss.vtl.impl.engine.statement;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class NamedComponentConstraint extends DataSetComponentConstraint
{
	private static final long serialVersionUID = 1L;

	private final Class<? extends Component> role;
	private final String domainName;
	
	public NamedComponentConstraint(String name, Class<? extends Component> role, String domainName)
	{
		super(name);

		this.role = role;
		this.domainName = domainName;
	}
	
	@Override
	protected Optional<Set<? extends DataStructureComponent<?, ?, ?>>> matchStructure(DataSetMetadata structure, MetadataRepository repo)
	{
		if (domainName != null)
			return structure.getComponent(getName(), role, repo.getDomain(domainName))
					.map(Collections::singleton);
		else
			return structure.getComponent(getName(), role)
					.map(Collections::singleton);
	}
	
	@Override
	public String toString()
	{
		return getName() + " " + role.getSimpleName().toLowerCase() 
				+ (domainName != null ? "<" + domainName + ">" : "");
	}
}
