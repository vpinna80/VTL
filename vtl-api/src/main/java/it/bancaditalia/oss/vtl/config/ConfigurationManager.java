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

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

public interface ConfigurationManager
{
	public enum VTLProperty {
		CONFIG_MANAGER("vtl.config.impl.class", "it.bancaditalia.oss.vtl.config.ConfigurationManagerImpl"),
		METADATA_REPOSITORY("vtl.metadatarepository.class", "it.bancaditalia.oss.vtl.impl.domains.InMemoryMetadataRepository"),
		ENGINE_IMPLEMENTATION("vtl.engine.implementation.class", "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine"),
		SESSION_IMPLEMENTATION("vtl.session.implementation.class", "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl");
		
		private final String property;
		private final String defaultValue;
		
		private VTLProperty(String property, String defaultValue)
		{
			this.property = property;
			this.defaultValue = defaultValue;
		}

		public String getValue()
		{
			return System.getProperty(property, defaultValue);
		}

		public String setValue(String newValue)
		{
			return System.setProperty(property, newValue);
		}
	}
	
	public static ConfigurationManager getDefault()  
	{
		return new ConfigurationManagerFactory().getDefault();
	}
	
	public MetadataRepository getMetadataRepositoryInstance();

	public VTLSession createSessionInstance();

	public Engine createEngineInstance();
}
