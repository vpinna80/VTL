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
package it.bancaditalia.oss.vtl.model.transform;

import java.util.Optional;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.Lineage;
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
	 * @throws VTLUnboundAliasException if the alias is not defined.
	 */
	public VTLValue resolve(String alias);

	/**
	 * Attempts to compute a given {@link Transformation}, binding references to aliases defined in this TransformationScheme.
	 *  
	 * @param rule The rule that must be evaluated.
	 * @return The {@link VTLValue} resulting from applying the given rule.
	 * @throws VTLUnboundAliasException if some references couldn't be resolved.
	 */
	public default VTLValue eval(Transformation rule)
	{
		return rule.eval(this);
	}

	/**
	 * Searches and retrieves metadata for value, referred by an alias defined in this TransformationScheme.
	 *  
	 * @param alias the alias whose value is to be retrieved.
	 * @return the {@link VTLValueMetadata metadata} of the value if the alias is found.
	 * @throws VTLUnboundAliasException if the alias is not defined.
	 */
	public VTLValueMetadata getMetadata(String alias);

	/**
	 * Returns a {@link Statement structure} a rule referred by an alias defined in this TransformationScheme.
	 * 
	 * @param alias the alias of the rule whose structure is to be retrieved.
	 * @return a {@link Statement} instance describing the rule if found.
	 * @throws VTLUnboundAliasException if the alias is not defined.
	 */
	public Statement getRule(String alias);

	/**
	 * Determine if an alias is defined in this TransformationScheme.
	 *  
	 * @param alias the alias whose value is to be retrieved.
	 * @return whether the alias is defined or not.
	 */
	public boolean contains(String alias);

	/**
	 * The same as {@code expected.cast(resolve(alias))} 
	 */
	public default <T extends VTLValue> T resolve(String alias, Class<T> expected)
	{
		return expected.cast(resolve(alias));
	}
	
	/**
	 * Checks if this transformation scheme is nested inside another one and return it.
	 * 
	 * @return The TransformationScheme encompassing this one, if any.
	 */
	public default TransformationScheme getParent()
	{
		return null;
	}

	/**
	 * Tries to persist a value in one of the environments managed by this scheme
	 * 
	 * @param value The value to persist
	 * @param alias The alias under which the value must be persisted
	 */
	public default void persist(VTLValue value, String alias) throws VTLException
	{
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Tries to resolve a lineage link specified by a given alias for a VTL rule defined in this scheme.
	 * 
	 * @param alias The alias referring to a rule
	 * @return The lineage link
	 */
	public Optional<Lineage> linkLineage(String alias);
}
