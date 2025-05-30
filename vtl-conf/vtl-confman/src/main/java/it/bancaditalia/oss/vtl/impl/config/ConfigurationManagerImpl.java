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
package it.bancaditalia.oss.vtl.impl.config;

import static it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory.instanceOfClass;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENGINE_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.SESSION_IMPLEMENTATION;
import static java.util.stream.Collectors.joining;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class ConfigurationManagerImpl implements ConfigurationManager
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationManagerImpl.class);

	@Override
	public MetadataRepository getMetadataRepository()
	{
		LOGGER.info("Initializing Metadata Repository " + METADATA_REPOSITORY.getValue());
		MetadataRepository repo = instanceOfClass(METADATA_REPOSITORY.getValue(), MetadataRepository.class, "Error initializing repository");
		return repo;
	}

	@Override
	public Engine getEngine()
	{
		LOGGER.info("Initializing VTL parser " + ENGINE_IMPLEMENTATION.getValue());
		Engine engine = instanceOfClass(ENGINE_IMPLEMENTATION.getValue(), Engine.class, "Error initializing engine");
		return engine;
	}

	@Override
	public VTLSession createSession(String code)
	{
		try
		{
			return Class.forName(SESSION_IMPLEMENTATION.getValue(), true, Thread.currentThread().getContextClassLoader()).asSubclass(VTLSession.class).getDeclaredConstructor(String.class).newInstance(code);
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException | NoSuchMethodException | SecurityException e)
		{
			throw new VTLNestedException("Error initializing session", e);
		}
		catch (InvocationTargetException e)
		{
			VTLNestedException wrapped = new VTLNestedException("Error initializing VTL session", e.getCause());
			LOGGER.error("Error while parsing the following VTL code: {}", code.lines().map(s -> s + "        ").collect(joining(System.lineSeparator())), e.getCause());
			throw wrapped;
		}
	}

	@Override
	public List<Environment> getEnvironments()
	{
		List<Environment> result = new ArrayList<>();

		List<String> envNames = ENVIRONMENT_IMPLEMENTATION.getValues();
		for (String envName: envNames)
			try 
			{
				LOGGER.info("Initializing VTL environment " + envName.replaceFirst("^.*\\.([^.]+)$", "$1"));
				result.add(instanceOfClass(envName, Environment.class, "Error initializing environment " + envName));
				LOGGER.info("Initialization of VTL environment " + envName.replaceFirst("^.*\\.([^.]+)$", "$1") + " complete");
			}
			catch (Exception e)
			{
				LOGGER.error("Error initializing environment " + envName, e.getCause());
			}

		return result;
	}
	
	@Override
	public Workspace createWorkspace()
	{
		return new WorkspaceImpl();
	}
}
