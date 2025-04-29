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

import static it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory.getSupportedProperties;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import io.sdmx.core.sdmx.manager.structure.SdmxRestToBeanRetrievalManager;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository;

/**
 * Composite metadata repository that queries a SDMX registry and fallbacks to a json file 
 * 
 * @author Valentino Pinna
 */
public class SDMXJsonRepository extends JsonMetadataRepository
{
	private static final long serialVersionUID = 1L;
	
	public final SdmxRestToBeanRetrievalManager rbrm;
	
	static
	{
		Set<VTLProperty> props = new HashSet<>(getSupportedProperties(SDMXRepository.class));
		props.addAll(ConfigurationManagerFactory.getSupportedProperties(JsonMetadataRepository.class));
		
		ConfigurationManagerFactory.registerSupportedProperties(SDMXJsonRepository.class, props.toArray(VTLProperty[]::new));
	}
	
	public SDMXJsonRepository() throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		super(new SDMXRepository());
		
		rbrm = ((SDMXRepository) getLinkedRepository()).rbrm;
	}
	
	public SDMXJsonRepository(String endpoint, String username, String password, URL jsonURL, Engine engine) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		super(new SDMXRepository(endpoint, username, password), jsonURL, engine);

		rbrm = ((SDMXRepository) getLinkedRepository()).rbrm;
	}
}
