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
package it.bancaditalia.oss.vtl.config;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

/**
 * Interface for the service used by the application to load and provide implementations
 * for the various VTL Engine components.
 * 
 * Instances of this interface are provided by {@link ConfigurationManagerFactory#getInstance()}.
 * 
 * @author Valentino Pinna
 */
public interface ConfigurationManager
{
	/**
	 * Same as {@link ConfigurationManagerFactory#getInstance()}.
	 * 
	 * @return a default instance of this interface.
	 */
	public static ConfigurationManager getDefault()  
	{
		return ConfigurationManagerFactory.getInstance();
	}

	/**
	 * @return The {@link MetadataRepository} instance
	 */
	public MetadataRepository getMetadataRepository();

	/**
	 * @return The {@link VTLSession} instance
	 */
	public VTLSession createSession();

	/**
	 * @return The {@link Engine} instance
	 */
	public Engine getEngine();

	/**
	 * @return The {@link List} of {@link Environment} instances
	 */
	public List<? extends Environment> getEnvironments();
	
	/**
	 * Load the current configuration from a provided Reader as a list of Java properties.
	 * 
	 * @param input The stream to read the properties from.
	 * @throws IOException
	 */
	public default void loadConfiguration(Reader input) throws IOException
	{
		throw new UnsupportedOperationException("Loading properties not supported.");
	}
	
	/**
	 * Saves the current configuration to the provided Writer as a list of Java properties.
	 * 
	 * @param output The stream to write the properties to.
	 * @throws IOException
	 */
	public default void saveConfiguration(Writer output) throws IOException
	{
		throw new UnsupportedOperationException("Saving properties not supported.");
	}
}
