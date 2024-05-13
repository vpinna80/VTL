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
package it.bancaditalia.oss.vtl.impl.meta.sdmx;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

/**
 * Composite metadata repository that queries a SDMX registry and fallbacks to a json file 
 * 
 * @author Valentino Pinna
 */
public class SDMXJsonRepository extends SDMXRepository
{
	private static final long serialVersionUID = 1L;
	
	static
	{
		Set<VTLProperty> props = new HashSet<>(ConfigurationManagerFactory.getSupportedProperties(SDMXRepository.class));
		props.addAll(ConfigurationManagerFactory.getSupportedProperties(JsonMetadataRepository.class));
		
		ConfigurationManagerFactory.registerSupportedProperties(SDMXJsonRepository.class, props.toArray(VTLProperty[]::new));
	}
	
	private final JsonMetadataRepository jsonRepo = new JsonMetadataRepository(); 

	public SDMXJsonRepository() throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		super();
	}
	
	@Override
	public DataSetMetadata getStructure(String alias)
	{
		return Optional.ofNullable(super.getStructure(alias)).orElseGet(() -> jsonRepo.getStructure(alias));
	}
	
	@Override
	public ValueDomainSubset<?, ?> getDomain(String alias)
	{
		return maybeGetDomain(alias).orElseGet(() -> jsonRepo.getDomain(alias));
	}
	
	@Override
	public boolean isDomainDefined(String domain)
	{
		return super.isDomainDefined(domain) || jsonRepo.isDomainDefined(domain);
	}
}
