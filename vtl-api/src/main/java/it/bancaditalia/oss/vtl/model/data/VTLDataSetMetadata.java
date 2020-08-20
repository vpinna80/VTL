package it.bancaditalia.oss.vtl.model.data;

import java.util.Collection;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.domain.StringCodeListDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;

public interface VTLDataSetMetadata extends VTLValueMetadata, DataStructure
{
	public void registerIndex(Set<? extends DataStructureComponent<Identifier, ?, ?>> keys);
	
	public Set<Set<? extends DataStructureComponent<Identifier, ?, ?>>> getRequestedIndexes();

	@Override
	public VTLDataSetMetadata swapComponent(DataStructureComponent<?, ?, ?> oldComponent, DataStructureComponent<?, ?, ?> newComponent);
	
	@Override
	public VTLDataSetMetadata keep(String... names);
	
	@Override
	public VTLDataSetMetadata membership(String name);
	
	@Override
	public VTLDataSetMetadata subspace(Collection<? extends DataStructureComponent<Identifier, ? extends ValueDomainSubset<? extends ValueDomain>, ? extends ValueDomain>> subspace);
	
	@Override
	public VTLDataSetMetadata joinForOperators(DataStructure other);
	
	@Override
	public VTLDataSetMetadata rename(DataStructureComponent<?, ?, ?> component, String newName);
	
	@Override
	public VTLDataSetMetadata drop(Collection<String> names);
	
	@Override
	public <S extends ValueDomainSubset<D>, D extends ValueDomain> VTLDataSetMetadata pivot(DataStructureComponent<Identifier, StringCodeListDomain, StringDomain> identifier,
			DataStructureComponent<Measure, S, D> measure);
}