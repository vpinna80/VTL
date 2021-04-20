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
package it.bancaditalia.oss.vtl.impl.domains;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
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

	@Override
	public ValueDomainSubset<?, ?> getDomain(String alias) 
	{
		if (domains.containsKey(alias))
			return domains.get(alias);
		
		throw new VTLException("Domain '" + alias + "' is undefined in the metadata.");
	}
	
	@Override
	public <S extends ValueDomainSubset<S, D>, D extends ValueDomain> S defineDomain(String alias, Class<S> domainClass, Object param)
	{
		return domainClass.cast(domains.computeIfAbsent(alias, a -> {
			try 
			{
				Class<? extends ValueDomain> implementingClass = mapClass(domainClass);
				Constructor<?>[] constructors = implementingClass.getConstructors();
				if (constructors.length == 0)
					throw new IllegalStateException("Implementation '" + implementingClass.getName() + "' of '" 
							+ domainClass + "' does not have any accessible constructor.");
				return domainClass.cast(constructors[0].newInstance(alias, param));
			} 
			catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) 
			{
				throw new VTLNestedException("Error defining domain " + alias, e);
			}
		}));
	}
	
	protected <S extends ValueDomainSubset<S, D>, D extends ValueDomain> BiConsumer<? super String, ? super Object> defineDomainOf(Class<S> domainClass)
	{
		return (alias, arg) -> defineDomain(alias, domainClass, arg); 
	}

	private Class<? extends ValueDomainSubset<?, ?>> mapClass(Class<? extends ValueDomain> domainClass)
	{
		if (domainClass == StringEnumeratedDomainSubset.class)
			return StringCodeList.class;
		else
			throw new UnsupportedOperationException(domainClass + " does not have a suitable implementation.");
	}
}
