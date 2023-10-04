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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl.of;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.XmlBody.xml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.xml.sax.SAXException;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.impl.meta.fmr.SDMXRepository;
import it.bancaditalia.oss.vtl.impl.meta.fmr.LazyCodeList;
import it.bancaditalia.oss.vtl.impl.meta.subsets.AbstractStringCodeList.StringCodeItemImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

@ExtendWith(MockServerExtension.class)
public class SdmxRepositoryTest
{
	private final MetadataRepository repo;

	public SdmxRepositoryTest(MockServerClient client) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		for (String[] entry: new String[][] { 
					{ "CL_CURRENCY.xml", "/codelist/ECB/CL_CURRENCY/1.0/" }, 
					{ "EXR.xml", "/dataflow/ECB/EXR/1.0/" }, 
					{ "ECB_EXR1.xml", "/datastructure/ECB/ECB_EXR1/1.0/" } 
				})
			try (InputStream resource = Objects.requireNonNull(SdmxRepositoryTest.class.getResourceAsStream(entry[0])))
			{
				client.when(request().withPath(entry[1])).respond(response().withBody(xml(IOUtils.toString(resource, "UTF-8"))));
			}

		System.setProperty("vtl.sdmx.meta.endpoint", "http://localhost:" + client.getPort());
		METADATA_REPOSITORY.setValue(SDMXRepository.class.getName());
		repo = ConfigurationManagerFactory.getInstance().getMetadataRepository();
	}

	@Test
	public void testGetCodes()
	{
		assertTrue(repo instanceof SDMXRepository);
		ValueDomainSubset<?, ?> domain = repo.getDomain("ECB:CL_CURRENCY(1.0)");
		assertTrue(domain instanceof LazyCodeList);
		Set<StringCodeItemImpl> codes = ((LazyCodeList) domain).getCodeItems();
		assertEquals(367, codes.size());
	}

	@Test
	public void testGetStructure()
	{
		DataSetMetadata expected = new DataStructureBuilder().addComponent(of("TIME_PERIOD", Identifier.class, TIMEDS)).addComponent(of("FREQ", Identifier.class, repo.getDomain("ECB:CL_FREQ(1.0)")))
				.addComponent(of("CURRENCY", Identifier.class, repo.getDomain("ECB:CL_CURRENCY(1.0)"))).addComponent(of("CURRENCY_DENOM", Identifier.class, repo.getDomain("ECB:CL_CURRENCY(1.0)")))
				.addComponent(of("EXR_TYPE", Identifier.class, repo.getDomain("ECB:CL_EXR_TYPE(1.0)"))).addComponent(of("EXR_SUFFIX", Identifier.class, repo.getDomain("ECB:CL_EXR_SUFFIX(1.0)")))
				.addComponent(of("OBS_VALUE", Measure.class, NUMBERDS)).addComponent(of("PUBL_ECB", Attribute.class, STRINGDS)).addComponent(of("DOM_SER_IDS", Attribute.class, STRINGDS))
				.addComponent(of("NAT_TITLE", Attribute.class, STRINGDS)).addComponent(of("TITLE_COMPL", Attribute.class, STRINGDS)).addComponent(of("TITLE", Attribute.class, STRINGDS))
				.addComponent(of("OBS_COM", Attribute.class, STRINGDS)).addComponent(of("SOURCE_AGENCY", Attribute.class, STRINGDS)).addComponent(of("UNIT_INDEX_BASE", Attribute.class, STRINGDS))
				.addComponent(of("SOURCE_PUB", Attribute.class, STRINGDS)).addComponent(of("UNIT_MULT", Attribute.class, STRINGDS)).addComponent(of("PUBL_MU", Attribute.class, STRINGDS))
				.addComponent(of("COVERAGE", Attribute.class, STRINGDS)).addComponent(of("TIME_FORMAT", Attribute.class, STRINGDS)).addComponent(of("BREAKS", Attribute.class, STRINGDS))
				.addComponent(of("PUBL_PUBLIC", Attribute.class, STRINGDS)).addComponent(of("DISS_ORG", Attribute.class, STRINGDS)).addComponent(of("OBS_CONF", Attribute.class, STRINGDS))
				.addComponent(of("COMPILING_ORG", Attribute.class, STRINGDS)).addComponent(of("COLLECTION", Attribute.class, STRINGDS)).addComponent(of("UNIT", Attribute.class, STRINGDS))
				.addComponent(of("COMPILATION", Attribute.class, STRINGDS)).addComponent(of("OBS_PRE_BREAK", Attribute.class, STRINGDS)).addComponent(of("DECIMALS", Attribute.class, STRINGDS))
				.addComponent(of("OBS_STATUS", Attribute.class, STRINGDS)).build();

		DataSetMetadata structure = repo.getStructure("ECB:EXR(1.0)");
		assertEquals(expected, structure);
	}
}
