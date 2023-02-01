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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class InMemoryMetadataRepository implements MetadataRepository, Serializable 
{
	private static final long serialVersionUID = 1L;

	private final Map<String, ValueDomainSubset<?, ?>> domains = new ConcurrentHashMap<>();
	
	public InMemoryMetadataRepository() 
	{
		for (Domains domain: EnumSet.allOf(Domains.class))
			domains.put(domain.name().toLowerCase(), domain.getDomain());
	}
	
	@Override
	public Collection<ValueDomainSubset<?, ?>> getValueDomains() 
	{
		return domains.values();
	}

	@Override
	public boolean isDomainDefined(String domain) 
	{
		return domains.containsKey(domain); 
	}

	protected Optional<ValueDomainSubset<?, ?>> getDomainOrNull(String alias)
	{
		return Optional.ofNullable(domains.get(alias));
	}

	@Override
	public ValueDomainSubset<?, ?> getDomain(String alias) 
	{
		if (domains.containsKey(requireNonNull(alias)))
			return domains.get(alias);
		
		throw new VTLException("Domain '" + alias + "' is undefined in the metadata.");
	}
	
	@Override
	public ValueDomainSubset<?, ?> defineDomain(String alias, ValueDomainSubset<?, ?> domain)
	{
		domains.putIfAbsent(requireNonNull(alias, "alias"), requireNonNull(domain, "domain"));
		return domains.get(alias);
	}
	
	@Override
	public DataSetMetadata getStructure(String name)
	{
		return null;
	}
}
