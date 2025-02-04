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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULL;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.meta.subsets.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
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

	private final Map<VTLAlias, ValueDomainSubset<?, ?>> domains = new ConcurrentHashMap<>();
	private final Map<ValueDomainSubset<?, ?>, Variable<?, ?>> defaultVars = new HashMap<>();
	private final Map<VTLAlias, Variable<?, ?>> vars = new ConcurrentHashMap<>();
	
	public InMemoryMetadataRepository() 
	{
		for (Domains domain: EnumSet.allOf(Domains.class))
		{
			ValueDomainSubset<?, ?> d = domain.getDomain();
			domains.put(VTLAliasImpl.of(domain.name()), d);
			if (domain != NULL)
				defaultVars.put(d, VariableImpl.of(VTLAliasImpl.of(d.getAlias() + "_var"), d));
		}
	}
	
	@Override
	public boolean isDomainDefined(VTLAlias domain) 
	{
		return domains.containsKey(domain); 
	}

	protected Optional<ValueDomainSubset<?, ?>> maybeGetDomain(VTLAlias alias)
	{
		return Optional.ofNullable(domains.get(alias));
	}

	@Override
	public ValueDomainSubset<?, ?> getDomain(VTLAlias alias) 
	{
		return maybeGetDomain(alias).orElseThrow(() -> new VTLUndefinedObjectException("Domain", alias));
	}
	
	@Override
	public void defineDomain(VTLAlias alias, ValueDomainSubset<?, ?> domain)
	{
		ValueDomainSubset<?, ?> prev = domains.putIfAbsent(requireNonNull(alias, "alias"), requireNonNull(domain, "domain"));
		if (prev != null && !prev.equals(domain))
			throw new VTLCastException(domain, prev);
	}
	
	@Override
	public Optional<VTLValueMetadata> getMetadata(VTLAlias name)
	{
		return Optional.empty();
	}
	
	@Override
	public HierarchicalRuleSet<?, ?, ?> getHierarchyRuleset(VTLAlias alias)
	{
		throw new VTLException("Hierarchical ruleset " + alias + " not found.");
	}
	
	@Override
	public DataPointRuleSet getDataPointRuleset(VTLAlias alias)
	{
		throw new VTLException("Data point ruleset " + alias + " not found.");
	}

	@Override
	public Variable<?, ?> getVariable(VTLAlias alias)
	{
		Variable<?, ?> instance = vars.get(alias);
		if (instance == null)
			throw new VTLUnboundAliasException(alias);
		
		return instance;
	}
	
	@Override
	public Variable<?, ?> createTempVariable(VTLAlias alias, ValueDomainSubset<?, ?> domain)
	{
		Variable<?, ?> variable = vars.get(alias);
		if (variable == null)
			return VariableImpl.of(alias, domain);
		else if (domain.equals(variable.getDomain()))
			return variable;
		else
			throw new VTLCastException(variable.getDomain(), domain);
	}

	@Override
	public String getDataSource(VTLAlias name)
	{
		return name.getName();
	}
}
