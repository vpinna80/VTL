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
package it.bancaditalia.oss.vtl.session;

import java.util.Collection;

import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

/**
 * A repository to contain and query all the defined domains.
 * 
 * @author Valentino Pinna
 */
public interface MetadataRepository
{
	/**
	 * @return a collection of all {@link ValueDomainSubset}s defined in this {@link MetadataRepository}.
	 */
	public Collection<ValueDomainSubset<?>> getValueDomains();
	
	/**
	 * Checks if a {@link ValueDomainSubset} with the specified name exists.
	 * 
	 * @param name the name of the domain to check
	 * @return true if a domain exists.
	 */
	public boolean isDomainDefined(String name);
	
	/**
	 * Returns a domain with the specified name if it exists.
	 * 
	 * @param name the name of the domain
	 * @return the domain or null if none exists.
	 */
	public ValueDomainSubset<?> getDomain(String name);
	
	/**
	 * Registers a new domain instance inside this repository (optional operation).
	 * 
	 * @param <T> the type of the domain
	 * @param name the name of the new domain
	 * @param domain the domain instance
	 * @return the same domain instance.
	 */
	public default <T extends ValueDomainSubset<?>> T registerDomain(String name, T domain)
	{
		throw new UnsupportedOperationException("registerDomain");
	}
	
	/**
	 * Creates a new domain instance and registers it inside this repository (optional operation).
	 * 
	 * @param <T> the type of the domain
	 * @param name the name of the new domain
	 * @param domainClass the class of the domain
	 * @param param optional parameters to pass to the domain class constructor
	 * 
	 * @return the created domain instance.
	 */
	public default <T extends ValueDomainSubset<?>> T defineDomain(String name, Class<T> domainClass, Object param)
	{
		throw new UnsupportedOperationException("defineDomain");
	}
	
	/**
	 * Initialize this {@link MetadataRepository}.
	 * 
	 * This method should be always called once per instance, before attempting any other operation.
	 * 
	 * @param params optional initialization parameters
	 * @return this instance
	 */
	public default MetadataRepository init(Object... params)
	{
		return this;
	}
}
