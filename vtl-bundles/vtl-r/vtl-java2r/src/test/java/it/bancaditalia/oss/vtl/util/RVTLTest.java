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
package it.bancaditalia.oss.vtl.util;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment.SDMX_DATA_ENDPOINT;
import static it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository.JSON_METADATA_URL;
import static it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository.SDMX_REGISTRY_ENDPOINT;

import java.io.IOException;
import java.io.StringReader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.VTLConfiguration;
import it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;

@SuppressWarnings("unused")
public class RVTLTest
{
	@BeforeAll
	private static void setProxy()
	{
		String proxyHost = System.getenv("RVTL_TEST_PROXY_HOST");
		String proxyPort = System.getenv("RVTL_TEST_PROXY_PORT");

		if (proxyHost != "")
		{
			System.setProperty("http.proxyHost", proxyHost);
			System.setProperty("https.proxyHost", proxyHost);
			System.setProperty("http.proxyPort", proxyPort);
			System.setProperty("https.proxyPort", proxyPort);
		}
	}
	
	@Test
	public void test() throws ClassNotFoundException, IOException
	{
//		VTLConfiguration conf = ConfigurationManager.newConfiguration();
//		conf.setPropertyValue(METADATA_REPOSITORY, "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXJsonRepository");
//		conf.setPropertyValue(ENVIRONMENT_IMPLEMENTATION, "it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment");
//		conf.setPropertyValue(SDMX_REGISTRY_ENDPOINT, "https://stats.bis.org/api/v1");
//		conf.setPropertyValue(SDMX_DATA_ENDPOINT, "https://stats.bis.org/api/v1");
//		conf.setPropertyValue(JSON_METADATA_URL, RVTLTest.class.getResource("ex_sdmx_dsd.json"));
//			
//		VTLSessionImpl session = new VTLSessionImpl(new StringReader("ds_sdmx := 1;"), conf);
//		session.compile();
//		session.resolve(VTLAliasImpl.of("ds_sdmx"));
	}
}
