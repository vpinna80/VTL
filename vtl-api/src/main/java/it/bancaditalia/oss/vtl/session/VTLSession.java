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
package it.bancaditalia.oss.vtl.session;

import java.util.List;
import java.util.Map;

import it.bancaditalia.oss.vtl.config.VTLConfiguration;
import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

/**
 * This interface provide the user with the ability to manage all VTL code, data and metadata 
 * by providing high-level methods that the user can interact with.
 * 
 * @author Valentino Pinna
 *
 */
public interface VTLSession extends TransformationScheme
{
	/**
	 * @return The list of environment instances configured for this session
	 */
	public Environment[] getEnvironments();

	/**
	 * @return The VTL engine used in this session.
	 */
	public Engine getEngine();

	/**
	 * Compile all the VTL code submitted to this session, and returns the metadata of
	 * all the objects contained in this session.
	 *  
	 * @return a {@link List} of {@link VTLValueMetadata} instances, describing a VTL object each.
	 */
	public Map<DMLStatement, VTLValueMetadata> compile();
	
	/**
	 * 
	 * @return The original code submitted when the session was created.
	 */
	public String getOriginalCode();

	/**
	 * Replace the code on this session with the provided code
	 * 
	 * @param code The new code.
	 */
	public void updateCode(String code);

	/**
	 * @return A list of all the statements resulting from the last compilation of this {@link VTLSession}
	 */
	public List<Statement> getRules();

	/**
	 * @return A copy of this {@link VTLSession}'s current configuration.
	 */
	public VTLConfiguration getConfiguration();
}