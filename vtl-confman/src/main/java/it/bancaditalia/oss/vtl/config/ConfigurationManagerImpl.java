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

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.VTLProperty.ENGINE_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.VTLProperty.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.VTLProperty.SESSION_IMPLEMENTATION;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class ConfigurationManagerImpl implements ConfigurationManager
{
	private final MetadataRepository metadataRepositoryInstance;
	private final Class<? extends VTLSession> sessionClass;
	private final Class<? extends Engine> engineClass;
	
	public ConfigurationManagerImpl() throws InstantiationException, IllegalAccessException, ClassNotFoundException 
	{
		metadataRepositoryInstance = Class.forName(METADATA_REPOSITORY.getValue()).asSubclass(MetadataRepository.class).newInstance();
		sessionClass = Class.forName(SESSION_IMPLEMENTATION.getValue()).asSubclass(VTLSession.class);
		engineClass = Class.forName(ENGINE_IMPLEMENTATION.getValue()).asSubclass(Engine.class);
	}

	@Override
	public MetadataRepository getMetadataRepositoryInstance()
	{
		return metadataRepositoryInstance;
	}

	@Override
	public VTLSession createSessionInstance()
	{
		try
		{
			return sessionClass.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e)
		{
			throw new VTLNestedException("Error initializing session", e);
		}
	}


	@Override
	public Engine createEngineInstance()
	{
		try
		{
			return engineClass.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e)
		{
			throw new VTLNestedException("Error initializing session", e);
		}
	}
}
