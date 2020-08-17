/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.domains;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringCodeListDomain;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class InMemoryMetadataRepository implements MetadataRepository, Serializable 
{
	private static final long serialVersionUID = 1L;

	protected static final ValueDomainSubset<?>[] INITIAL_DOMAINS = new ValueDomainSubset<?>[] { NUMBERDS, INTEGERDS, STRINGDS, BOOLEANDS, TIMEDS, DATEDS };

	private final Map<String, ValueDomainSubset<? extends ValueDomain>> domains = new ConcurrentHashMap<>();
	
	public InMemoryMetadataRepository() 
	{
		for (ValueDomainSubset<?> domain: INITIAL_DOMAINS)
			domains.put(domain.toString().toLowerCase(), domain);
	}
	
	@Override
	public Collection<ValueDomainSubset<?>> getValueDomains() 
	{
		return domains.values();
	}

	@Override
	public boolean isDomainDefined(String alias) 
	{
		return domains.containsKey(mapAlias(alias)); 
	}

	@Override
	public ValueDomainSubset<?> getDomain(String alias) 
	{
		if (domains.containsKey(mapAlias(alias)))
			return domains.get(mapAlias(alias));
		
		throw new VTLException("Domain '" + alias + "' is undefined in the metadata.");
	}
	
	@Override
	public <T extends ValueDomainSubset<?>> T defineDomain(String alias, Class<T> domainClass, Object arg) 
	{
		return domainClass.cast(domains.computeIfAbsent(mapAlias(alias), a -> {
			try 
			{
				Class<? extends ValueDomain> implementingClass = mapClass(domainClass);
				Constructor<?>[] constructors = implementingClass.getConstructors();
				if (constructors.length == 0)
					throw new IllegalStateException("Implementation '" + implementingClass.getName() + "' of '" 
							+ domainClass + "' does not have any accessible constructor.");
				return domainClass.cast(constructors[0].newInstance(alias, arg));
			} 
			catch (Exception e) 
			{
				throw new IllegalStateException(e);
			}
		}));
	}
	
	public BiConsumer<? super String, ? super Object> defineDomainOf(Class<? extends ValueDomainSubset<?>> domainClass)
	{
		return (alias, arg) -> defineDomain(alias, domainClass, arg); 
	}

	private Class<? extends ValueDomainSubset<?>> mapClass(Class<? extends ValueDomain> domainClass)
	{
		if (domainClass == StringCodeListDomain.class)
			return StringCodeListDomainImpl.class;
		else
			throw new UnsupportedOperationException(domainClass + " does not have a suitable implementation.");
	}

	private String mapAlias(String alias) 
	{
		return alias.matches("^'.*'$") ? alias.replaceAll("^'(.*)'$", "$1") : alias.toLowerCase();
	}
}
