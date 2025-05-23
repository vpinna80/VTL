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

import java.util.Map;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
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
	public VTLValue resolve(VTLAlias alias);

	/**
	 * The same as {@code expected.cast(resolve(alias))} 
	 */
	public default <T extends VTLValue> T resolve(VTLAlias alias, Class<T> expected)
	{
		return expected.cast(resolve(alias));
	}

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
	public VTLValueMetadata getMetadata(VTLAlias alias);

	/**
	 * Returns a {@link Statement structure} a rule referred by an alias defined in this TransformationScheme.
	 * 
	 * @param alias the alias of the rule whose structure is to be retrieved.
	 * @return a {@link Statement} instance describing the rule if found.
	 * @throws VTLUnboundAliasException if the alias is not defined.
	 */
	public Statement getRule(VTLAlias alias);

	/**
	 * Returns a holder map for computed values of type T
	 *  
	 * @param <T> The type of the holder
	 * @param type The type of the holder
	 * @return The holder for computed values of type T
	 */
	public <T> Map<Transformation, T> getResultHolder(Class<T> type);
	
	/**
	 * Returns a {@link HierarchicalRuleSet} referred by an alias defined in this TransformationScheme.
	 * 
	 * @param alias the alias of the rule whose structure is to be retrieved.
	 * @return a {@link HierarchicalRuleSet} instance describing the rule if found.
	 * @throws VTLUnboundAliasException if the alias is not defined.
	 */
	public HierarchicalRuleSet findHierarchicalRuleset(VTLAlias alias);

	/**
	 * Returns a {@link DataPointRuleSet} referred by an alias defined in this TransformationScheme.
	 * 
	 * @param alias the alias of the ruleset
	 * @return a {@link DataPointRuleSet} instance describing the rule if found.
	 * @throws VTLUnboundAliasException if the alias is not defined.
	 */
	public DataPointRuleSet findDatapointRuleset(VTLAlias alias);

	/**
	 * Tries to persist a value in one of the environments managed by this scheme
	 * 
	 * @param value The value to persist
	 * @param alias The alias under which the value must be persisted
	 */
	public default void persist(VTLValue value, VTLAlias alias) throws VTLException
	{
		throw new UnsupportedOperationException();
	}
}
