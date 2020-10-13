package it.bancaditalia.oss.vtl.impl.engine.statement;

import static it.bancaditalia.oss.vtl.impl.engine.statement.AnonymousComponentConstraint.QuantifierConstraints.ANY;
import static it.bancaditalia.oss.vtl.impl.engine.statement.AnonymousComponentConstraint.QuantifierConstraints.AT_LEAST_ONE;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class AnonymousComponentConstraint extends DataSetComponentConstraint
{
	private static final long serialVersionUID = 1L;

	private final Class<? extends Component> role;
	private final String domainName;
	private final QuantifierConstraints quantifier;
	
	public enum QuantifierConstraints 
	{
		MAX_ONE, AT_LEAST_ONE, ANY
	}

	public AnonymousComponentConstraint(Class<? extends Component> role, String domainName, QuantifierConstraints quantifier)
	{
		super(null);

		this.role = role;
		this.domainName = domainName;
		this.quantifier = quantifier;
	}

	@Override
	protected Optional<Set<? extends DataStructureComponent<?, ?, ?>>> matchStructure(DataSetMetadata structure, MetadataRepository repo)
	{
		Set<? extends DataStructureComponent<?, ?, ?>> matchedComponents;
		if (domainName != null)
			matchedComponents = structure.getComponents(role, repo.getDomain(domainName));
		else
			matchedComponents = structure.getComponents(role);
		
		switch (quantifier)
		{
			case ANY:
				return Optional.of(matchedComponents);
			case AT_LEAST_ONE:
				return matchedComponents.size() > 0 ? Optional.of(matchedComponents) : Optional.empty();
			case MAX_ONE:
				return matchedComponents.size() > 0 ? Optional.of(Collections.singleton(matchedComponents.iterator().next())) : Optional.empty();
			default:
				throw new IllegalStateException("Unknown quantifier");
		}
	}
	
	@Override
	public String toString()
	{
		return role.getSimpleName().toLowerCase() 
				+ (domainName != null ? "<" + domainName + ">" : "")
				+ " _ " + (quantifier == ANY ? "*" : quantifier == AT_LEAST_ONE ? "+" : "");
	}
}
