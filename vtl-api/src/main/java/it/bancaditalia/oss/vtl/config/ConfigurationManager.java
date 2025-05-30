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
import java.util.List;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

/**
 * Interface for the service used by the application to load and provide implementations
 * for the various VTL Engine components.
 * 
 * Instances of this interface are provided by {@link ConfigurationManagerFactory#newManager()}.
 * 
 * @author Valentino Pinna
 */
public interface ConfigurationManager
{
	/**
	 * Creates a VTL session from the passed VTL code. 
	 * 
	 * @param code The VTL code to base to this session on.
	 * @return The {@link VTLSession} instance.
	 */
	public VTLSession createSession(String code);

	/**
	 * Creates a VTL session from the passed VTL code.
	 * 
	 * @param reader the {@link Reader} instance to read the VTL code from. It will be closed when the script is read.
	 * @return The {@link VTLSession} instance.
	 */
	public default VTLSession createSession(Reader reader) throws IOException
	{
		try (BufferedReader br = new BufferedReader(reader))
		{
			return createSession(br.lines().collect(joining(lineSeparator(), "", lineSeparator())));
		}
	}

	/**
	 * @return The {@link MetadataRepository} instance
	 */
	public MetadataRepository getMetadataRepository();

	/**
	 * @return The {@link Engine} instance
	 */
	public Engine getEngine();

	/**
	 * @return The {@link List} of {@link Environment} instances
	 */
	public List<Environment> getEnvironments();
	
	/**
	 * Creates a new, empty workspace.
	 * @return The newly created {@link Workspace} instance
	 */
	public Workspace createWorkspace();
}
