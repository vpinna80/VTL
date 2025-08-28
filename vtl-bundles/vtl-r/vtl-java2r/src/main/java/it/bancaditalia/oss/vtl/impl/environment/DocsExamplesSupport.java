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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.newConfiguration;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.impl.environment.docs.VTLExamplesEnvironment.EXAMPLES_CATEGORY;
import static it.bancaditalia.oss.vtl.impl.environment.docs.VTLExamplesEnvironment.EXAMPLES_OPERATOR;
import static it.bancaditalia.oss.vtl.impl.environment.docs.VTLExamplesEnvironment.computeJsonURL;
import static it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository.JSON_METADATA_URL;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.VTLConfiguration;
import it.bancaditalia.oss.vtl.impl.environment.docs.VTLExamplesEnvironment;
import it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository;
import it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class DocsExamplesSupport
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RUtils.class);
	private static final Map<Entry<String, String>, VTLSession> SESSIONS = new HashMap<>();

	private DocsExamplesSupport() {}
	
	public static synchronized VTLSession createSession(String category, String operator)
	{
		return SESSIONS.computeIfAbsent(new SimpleEntry<>(category, operator), k -> createExample(category, operator));
	}
	
	private static Reader retrieveCode(String category, String operator) throws IOException
	{
		return new InputStreamReader(VTLExamplesEnvironment.computeCodeURL(category, operator).openStream(), UTF_8);
	}

	private static VTLSession createExample(String category, String operator)
	{
		try
		{
			LOGGER.info("Initializing metadata for {}", operator);
			VTLConfiguration config = newConfiguration();
			config.setPropertyValue(METADATA_REPOSITORY, JsonMetadataRepository.class);
			config.setPropertyValue(JSON_METADATA_URL, computeJsonURL(category, operator));
			config.setPropertyValue(ENVIRONMENT_IMPLEMENTATION, VTLExamplesEnvironment.class);
			config.setPropertyValue(EXAMPLES_CATEGORY, category);
			config.setPropertyValue(EXAMPLES_OPERATOR, operator);
			
			return new VTLSessionImpl(retrieveCode(category, operator), config);
		}
		catch (IOException | ClassNotFoundException | SecurityException | IllegalArgumentException e)
		{
			throw new RuntimeException(e);
		}
	}
}
