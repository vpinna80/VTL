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
import it.bancaditalia.oss.vtl.impl.meta.subsets.AbstractStringCodeList.StringCodeItemImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

@ExtendWith(MockServerExtension.class)
public class FMRRepositoryTest
{
	private final MetadataRepository repo;

	public FMRRepositoryTest(MockServerClient client) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		try (InputStream resource = Objects.requireNonNull(FMRRepositoryTest.class.getResourceAsStream("CL_CURRENCY.xml")))
		{
			client.when(request().withPath("/codelist/ECB/CL_CURRENCY/1.0/")).respond(response().withBody(xml(IOUtils.toString(resource, "UTF-8"))));
		}
		try (InputStream resource = Objects.requireNonNull(FMRRepositoryTest.class.getResourceAsStream("EXR.xml")))
		{
			client.when(request().withPath("/dataflow/ECB/EXR/1.0/")).respond(response().withBody(xml(IOUtils.toString(resource, "UTF-8"))));
		}
		try (InputStream resource = Objects.requireNonNull(FMRRepositoryTest.class.getResourceAsStream("ECB_EXR1.xml")))
		{
			client.when(request().withPath("/datastructure/ECB/ECB_EXR1/1.0/")).respond(response().withBody(xml(IOUtils.toString(resource, "UTF-8"))));
		}

		System.setProperty("vtl.fmr.endpoint", "http://localhost:" + client.getPort());
		METADATA_REPOSITORY.setValue(FMRRepository.class.getName());
		repo = ConfigurationManagerFactory.getInstance().getMetadataRepository();
	}

	@Test
	public void testGetCodes()
	{
		assertTrue(repo instanceof FMRRepository);
		ValueDomainSubset<?, ?> domain = repo.getDomain("ECB:CL_CURRENCY(1.0)");
		assertTrue(domain instanceof LazyCodeList);
		Set<StringCodeItemImpl> codes = ((LazyCodeList) domain).getCodeItems();
		assertEquals(367, codes.size());
	}

	@Test
	public void testGetStructure()
	{
		DataSetMetadata expected = new DataStructureBuilder().addComponent(of("time_period", Identifier.class, TIMEDS)).addComponent(of("freq", Identifier.class, repo.getDomain("ECB:CL_FREQ(1.0)")))
				.addComponent(of("currency", Identifier.class, repo.getDomain("ECB:CL_CURRENCY(1.0)"))).addComponent(of("currency_denom", Identifier.class, repo.getDomain("ECB:CL_CURRENCY(1.0)")))
				.addComponent(of("exr_type", Identifier.class, repo.getDomain("ECB:CL_EXR_TYPE(1.0)"))).addComponent(of("exr_suffix", Identifier.class, repo.getDomain("ECB:CL_EXR_SUFFIX(1.0)")))
				.addComponent(of("obs_value", Measure.class, NUMBERDS)).addComponent(of("publ_ecb", Attribute.class, STRINGDS)).addComponent(of("dom_ser_ids", Attribute.class, STRINGDS))
				.addComponent(of("nat_title", Attribute.class, STRINGDS)).addComponent(of("title_compl", Attribute.class, STRINGDS)).addComponent(of("title", Attribute.class, STRINGDS))
				.addComponent(of("obs_com", Attribute.class, STRINGDS)).addComponent(of("source_agency", Attribute.class, STRINGDS)).addComponent(of("unit_index_base", Attribute.class, STRINGDS))
				.addComponent(of("source_pub", Attribute.class, STRINGDS)).addComponent(of("unit_mult", Attribute.class, STRINGDS)).addComponent(of("publ_mu", Attribute.class, STRINGDS))
				.addComponent(of("coverage", Attribute.class, STRINGDS)).addComponent(of("time_format", Attribute.class, STRINGDS)).addComponent(of("breaks", Attribute.class, STRINGDS))
				.addComponent(of("publ_public", Attribute.class, STRINGDS)).addComponent(of("diss_org", Attribute.class, STRINGDS)).addComponent(of("obs_conf", Attribute.class, STRINGDS))
				.addComponent(of("compiling_org", Attribute.class, STRINGDS)).addComponent(of("collection", Attribute.class, STRINGDS)).addComponent(of("unit", Attribute.class, STRINGDS))
				.addComponent(of("compilation", Attribute.class, STRINGDS)).addComponent(of("obs_pre_break", Attribute.class, STRINGDS)).addComponent(of("decimals", Attribute.class, STRINGDS))
				.addComponent(of("obs_status", Attribute.class, STRINGDS)).build();

		DataSetMetadata structure = repo.getStructure("ECB:EXR(1.0)");
		assertEquals(expected, structure);
	}
}
