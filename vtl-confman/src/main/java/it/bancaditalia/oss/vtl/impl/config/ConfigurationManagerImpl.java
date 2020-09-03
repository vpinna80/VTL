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
package it.bancaditalia.oss.vtl.impl.config;

import static it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory.instanceOfClass;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENGINE_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.SESSION_IMPLEMENTATION;

import java.util.ArrayList;
import java.util.List;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class ConfigurationManagerImpl implements ConfigurationManager
{
	private final MetadataRepository metadataRepositoryInstance;

	public ConfigurationManagerImpl() 
	{
		metadataRepositoryInstance = instanceOfClass(METADATA_REPOSITORY.getValue(), MetadataRepository.class, "Error initializing repository");
	}

	@Override
	public MetadataRepository getMetadataRepository()
	{
		return metadataRepositoryInstance;
	}

	@Override
	public Engine getEngine()
	{
		return instanceOfClass(ENGINE_IMPLEMENTATION.getValue(), Engine.class, "Error initializing engine");
	}

	@Override
	public VTLSession createSession()
	{
		return instanceOfClass(SESSION_IMPLEMENTATION.getValue(), VTLSession.class, "Error initializing session");
	}

	@Override
	public List<Environment> getEnvironments()
	{
		List<Environment> result = new ArrayList<>();

		String[] envNames = ENVIRONMENT_IMPLEMENTATION.getValue().split(",");
		for (int i = 0; i < envNames.length; i++)
			result.add(instanceOfClass(envNames[i], Environment.class, "Error initializing enironment " + envNames[i]));

		return result ;
	}
}
