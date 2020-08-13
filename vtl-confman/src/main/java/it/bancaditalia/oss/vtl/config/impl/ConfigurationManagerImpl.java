/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.config;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENGINE_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.SESSION_IMPLEMENTATION;

import java.util.ArrayList;
import java.util.List;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class ConfigurationManagerImpl implements ConfigurationManager
{
	private final MetadataRepository metadataRepositoryInstance;

	public ConfigurationManagerImpl() 
	{
		try
		{
			metadataRepositoryInstance = Class.forName(METADATA_REPOSITORY.getValue()).asSubclass(MetadataRepository.class).newInstance();
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
		{
			throw new VTLNestedException("Error loading implementations", e);
		}
	}

	@Override
	public MetadataRepository getMetadataRepository()
	{
		return metadataRepositoryInstance;
	}

	@Override
	public Engine getEngine()
	{
		try
		{
			return Class.forName(ENGINE_IMPLEMENTATION.getValue()).asSubclass(Engine.class).newInstance();
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
		{
			throw new VTLNestedException("Error initializing engine", e);
		}
	}

	@Override
	public VTLSession createSession()
	{
		try
		{
			return Class.forName(SESSION_IMPLEMENTATION.getValue()).asSubclass(VTLSession.class).newInstance();
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
		{
			throw new VTLNestedException("Error initializing session", e);
		}
	}

	@Override
	public List<Environment> getEnvironments()
	{
		try
		{
			List<Environment> result = new ArrayList<>();

			String[] envNames = ENVIRONMENT_IMPLEMENTATION.getValue().split(",");
			for (int i = 0; i < envNames.length; i++)
				result.add(Class.forName(envNames[i]).asSubclass(Environment.class).newInstance()); 

			return result ;
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
		{
			throw new VTLNestedException("Error loading implementations", e);
		}
	}
}
