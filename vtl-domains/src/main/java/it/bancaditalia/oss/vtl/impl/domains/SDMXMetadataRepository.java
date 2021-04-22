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

import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.util.Utils;

public class SDMXMetadataRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SDMXMetadataRepository.class);

	public static final VTLProperty METADATA_SDMX_PROVIDER_ENDPOINT = 
			new VTLPropertyImpl("vtl.metadata.sdmx.provider.endpoint", "SDMX service provider endpoint", "https://www.myurl.com/service", true);
	
	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(SDMXMetadataRepository.class, METADATA_SDMX_PROVIDER_ENDPOINT);
	}

	public SDMXMetadataRepository() throws IOException, SAXException, ParserConfigurationException
	{
		String url = METADATA_SDMX_PROVIDER_ENDPOINT.getValue();
		if (url == null)
			throw new IllegalStateException("No endpoint configured for SDMX metadata repository.");
		
		LOGGER.info("Loading metadata from {}", url);
		url += "/codelist";
		
		LOGGER.debug("Parsing metadata from {}...", url);
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		URLConnection conn = new URL(url).openConnection();
		Document document = factory.newDocumentBuilder().parse(new InputSource(new InputStreamReader(conn.getInputStream(), 
				conn.getContentEncoding() == null ? "utf-8" : conn.getContentEncoding())));
		
		LOGGER.debug("Preparing codelists...", url);
		NodeList codelists = document.getElementsByTagNameNS("http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure", "Codelist");
		
		LOGGER.debug("Creating {} codelists...", codelists.getLength());
		Utils.getStream(codelists.getLength())
			.mapToObj(codelists::item)
			.map(Element.class::cast)
			.forEach(codelist -> {
				LOGGER.trace("Populating codelist {}", codelist.getAttribute("id"));
				NodeList codes = codelist.getElementsByTagNameNS("http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure", "Code");
				LOGGER.trace("Codelist {} has {} codes", codes.getLength());
				Set<String> items = Utils.getStream(codes.getLength())
					.mapToObj(codes::item)
					.map(Element.class::cast)
					.map(code -> code.getAttribute("id"))
					.collect(toSet());
				defineDomain(codelist.getAttribute("id"), StringEnumeratedDomainSubset.class, items);
			});
		LOGGER.info("Finished loading metadata", url);
	}
}
