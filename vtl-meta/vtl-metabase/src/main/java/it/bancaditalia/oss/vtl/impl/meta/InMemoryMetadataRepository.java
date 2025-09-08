/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
package it.bancaditalia.oss.vtl.impl.meta;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataStructureDefinition;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class InMemoryMetadataRepository implements MetadataRepository, Serializable 
{
	private static final long serialVersionUID = 1L;
	private static final Map<VTLAlias, ValueDomainSubset<?, ?>> DOMAINS = new HashMap<>();
	private static final Map<VTLAlias, Variable<?, ?>> DEFAULT_VARS = new HashMap<>();

	static
	{
		for (Domains domain: EnumSet.allOf(Domains.class))
			if (domain != Domains.NULL)
			{
				ValueDomainSubset<?, ?> d = domain.getDomain();
				DOMAINS.put(VTLAliasImpl.of(domain.name()), d);
				DataSetComponent<Measure, ?, ?> comp = DataSetComponentImpl.getDefaultMeasure(d);
				Variable<?, ?> defaultVariable = VariableImpl.of(comp.getAlias(), d);
				DEFAULT_VARS.put(defaultVariable.getAlias(), defaultVariable);
			}
	}
	
	private final MetadataRepository chained;
	
	public InMemoryMetadataRepository()
	{
		this(null);
	}
	
	public InMemoryMetadataRepository(MetadataRepository chained)
	{
		this.chained = chained;
	}

	@Override
	public Optional<ValueDomainSubset<?, ?>> getDomain(VTLAlias alias) 
	{
		return Optional.<ValueDomainSubset<?, ?>>ofNullable(DOMAINS.get(alias))
				.or(() -> Optional.ofNullable(chained).flatMap(chained -> chained.getDomain(alias)));
	}
	
	@Override
	public Optional<VTLValueMetadata> getMetadata(VTLAlias alias)
	{
		return Optional.ofNullable(chained).flatMap(chained -> chained.getMetadata(alias));
	}
	
	@Override
	public Optional<HierarchicalRuleSet> getHierarchyRuleset(VTLAlias alias)
	{
		return Optional.ofNullable(chained).flatMap(chained -> chained.getHierarchyRuleset(alias));
	}
	
	@Override
	public Optional<DataPointRuleSet> getDataPointRuleset(VTLAlias alias)
	{
		return Optional.ofNullable(chained).flatMap(chained -> chained.getDataPointRuleset(alias));
	}

	@Override
	public Optional<Variable<?, ?>> getVariable(VTLAlias alias)
	{
		return Optional.<Variable<?, ?>>ofNullable(DEFAULT_VARS.get(alias))
				.or(() -> Optional.ofNullable(chained).flatMap(chained -> chained.getVariable(alias)));
	}

	@Override
	public String getDataSource(VTLAlias alias)
	{
		return Optional.ofNullable(chained).map(chained -> chained.getDataSource(alias)).orElse(alias.getName());
	}

	@Override
	public Optional<DataStructureDefinition> getStructureDefinition(VTLAlias alias)
	{
		return Optional.ofNullable(chained).flatMap(chained -> chained.getStructureDefinition(alias));
	}

	@Override
	public Optional<String> getTransformationScheme(VTLAlias alias)
	{
		return chained == null ? Optional.empty() : chained.getTransformationScheme(alias);
	}
	
	@Override
	public Set<VTLAlias> getAvailableSchemeAliases()
	{
		return chained == null ? Set.of() : chained.getAvailableSchemeAliases();
	}
	
	@Override
	public MetadataRepository getLinkedRepository()
	{
		return chained;
	}
}
