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
package it.bancaditalia.oss.vtl.model.transform;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundNameException;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

/**
 * A Transformation Scheme, as defined by VTL specification, is a collection of VTL statements 
 * linked together, that are meant to be evaluated within the same scope.
 * 
 * @author Valentino Pinna
 */
public interface TransformationScheme
{
	/**
	 * @return The {@link MetadataRepository} instance used by this TransformationScheme. 
	 */
	public MetadataRepository getRepository();

	/**
	 * Searches and retrieves a value, referred by an alias defined in this TransformationScheme.
	 *  
	 * @param alias The alias whose value is to be retrieved.
	 * @return The {@link VTLValue} if the alias is found.
	 * @throws VTLUnboundNameException if the alias is not defined.
	 */
	public VTLValue resolve(String alias);

	/**
	 * Searches and retrieves metadata for value, referred by an alias defined in this TransformationScheme.
	 *  
	 * @param alias the alias whose value is to be retrieved.
	 * @return the {@link VTLValueMetadata metadata} of the value if the alias is found.
	 * @throws VTLUnboundNameException if the alias is not defined.
	 */
	public VTLValueMetadata getMetadata(String alias);

	/**
	 * Returns a {@link Statement structure} a rule referred by an alias defined in this TransformationScheme.
	 * 
	 * @param alias the alias of the rule whose structure is to be retrieved.
	 * @return a {@link Statement} instance describing the rule if found.
	 * @throws VTLUnboundNameException if the alias is not defined.
	 */
	public Statement getRule(String alias);

	/**
	 * The same as {@code expected.cast(resolve(alias))} 
	 */
	public default <T extends VTLValue> T resolve(String alias, Class<T> expected)
	{
		return expected.cast(resolve(alias));
	}
}
