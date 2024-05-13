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

import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;

import java.io.BufferedReader;
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
	 * @return The {@link MetadataRepository} instance
	 */
	public MetadataRepository getMetadataRepository();

	/**
	 * @return The {@link VTLSession} instance
	 */
	public VTLSession createSession(String code);

	/**
	 * @return The {@link VTLSession} instance
	 */
	public default VTLSession createSession(Reader reader) throws IOException
	{
		try (BufferedReader br = new BufferedReader(reader))
		{
			return createSession(br.lines().collect(joining(lineSeparator(), "", lineSeparator())));
		}
	}

	/**
	 * @return The {@link Engine} instance
	 */
	public Engine getEngine();

	/**
	 * @return The {@link List} of {@link Environment} instances
	 */
	public List<Environment> getEnvironments();
	
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
