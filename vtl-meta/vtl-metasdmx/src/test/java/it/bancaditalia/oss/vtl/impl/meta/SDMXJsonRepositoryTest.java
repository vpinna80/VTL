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

import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.TIME_PERIOD;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.EnumSet.allOf;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.junit.jupiter.MockServerSettings;
import org.mockserver.model.StringBody;
import org.xml.sax.SAXException;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXJsonRepository;
import it.bancaditalia.oss.vtl.impl.meta.sdmx.SdmxCodeList;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList.StringCodeItem;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

@ExtendWith(MockServerExtension.class)
@MockServerSettings(ports = { 38766 })
public class SDMXJsonRepositoryTest
{
	private final MetadataRepository repo;

	public SDMXJsonRepositoryTest(MockServerClient client) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		for (String[] entry: new String[][] { 
			{ "codelists.xml", "/codelist/all/all/all/" },
			{ "dsds.xml", "/datastructure/all/all/latest/" },
			{ "dataflows.xml", "/dataflow/all/all/latest/" },
			{ "schemes.xml", "/transformationscheme/all/all/latest/" }
		}) try (InputStream resource = requireNonNull(SDMXJsonRepositoryTest.class.getResourceAsStream(entry[0])))
			{
				client.when(request().withPath(entry[1]), exactly(1)).respond(response().withBody(new StringBody(IOUtils.toString(resource, "UTF-8"))));
			}

		URL jsonURL = requireNonNull(SDMXJsonRepositoryTest.class.getResource("test.json"));
		repo = new SDMXJsonRepository("http://localhost:" + client.getPort(), null, null, jsonURL, mock(Engine.class));
	}

	@Test
	public void testGetCodes(MockServerClient client) throws IOException
	{
		assertTrue(repo instanceof SDMXJsonRepository);
		ValueDomainSubset<?, ?> domain = repo.getDomain(VTLAliasImpl.of(true, "ECB:CL_CURRENCY(1.0)")).get();
		assertTrue(domain instanceof SdmxCodeList);
		Set<StringCodeItem> codes = ((SdmxCodeList) domain).getCodeItems();
		assertEquals(369, codes.size());

		assertTrue(repo instanceof SDMXJsonRepository);
		domain = repo.getDomain(VTLAliasImpl.of(true, "VD_1")).get();
		assertTrue(domain instanceof StringCodeList);
		codes = ((StringCodeList) domain).getCodeItems();
		assertEquals(10, codes.size());
	}

	@Test
	public void testGetSdmxStructure(MockServerClient client) throws IOException
	{
		assertTrue(repo instanceof SDMXJsonRepository);
		VTLValueMetadata actual = repo.getMetadata(VTLAliasImpl.of(true, "ECB:EXR(1.0)")).orElseThrow(() -> new NullPointerException());
		DataSetMetadata expected = new DataStructureBuilder()
				.addComponents(TIME_PERIOD)
				.addComponents(allOf(TestComponents.class).stream().map(c -> c.get(repo)).collect(toList()))
				.build();
		
		assertInstanceOf(DataSetMetadata.class, actual);
		for (DataStructureComponent<?, ?, ?> c: (DataSetMetadata) actual)
			if (!expected.contains(c))
				assertTrue(expected.contains(c), c + " not found in " + expected);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetJsonStructure(MockServerClient client) throws IOException
	{
		assertTrue(repo instanceof SDMXJsonRepository);
		VTLValueMetadata actual = repo.getMetadata(VTLAliasImpl.of(true, "DS_1")).orElseThrow(() -> new NullPointerException());
		DataSetMetadata expected = new DataStructureBuilder()
				.addComponent(repo.createTempVariable(VTLAliasImpl.of("Id_1"), STRINGDS).as(Identifier.class))
				.addComponent(repo.createTempVariable(VTLAliasImpl.of("Me_1"), NUMBERDS).as(Measure.class))
				.addComponent(repo.createTempVariable(VTLAliasImpl.of("At_1"), repo.getDomain(VTLAliasImpl.of("'ECB:CL_CURRENCY(1.0)'")).get()).as(Attribute.class))
				.addComponent(repo.createTempVariable(VTLAliasImpl.of("Va_1"), repo.getDomain(VTLAliasImpl.of("VD_1")).get()).as(ViralAttribute.class))
				.build();
		
		assertInstanceOf(DataSetMetadata.class, actual);
		for (DataStructureComponent<?, ?, ?> c: (DataSetMetadata) actual)
			if (!expected.contains(c))
				assertTrue(expected.contains(c), c + " not found in " + expected);
		assertEquals(expected, actual);
	}
}
