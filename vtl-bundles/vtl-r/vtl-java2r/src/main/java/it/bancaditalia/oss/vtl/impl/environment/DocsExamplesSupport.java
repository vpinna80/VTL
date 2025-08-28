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
