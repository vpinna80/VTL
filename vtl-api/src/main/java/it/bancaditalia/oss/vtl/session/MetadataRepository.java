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

import java.util.Collection;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

/**
 * A repository to contain and query all the defined domains.
 * 
 * @author Valentino Pinna
 */
public interface MetadataRepository
{
	/**
	 * @return a collection of all {@link ValueDomainSubset}s defined in this {@link MetadataRepository}.
	 */
	public Collection<ValueDomainSubset<?, ?>> getValueDomains();
	
	/**
	 * Checks if a {@link ValueDomainSubset} with the specified name exists.
	 * 
	 * @param name the name of the domain to check
	 * @return true if a domain exists.
	 */
	public boolean isDomainDefined(String name);
	
	/**
	 * Returns a domain with the specified name if it exists.
	 * 
	 * @param name the name of the domain
	 * @return the domain or null if none exists.
	 */
	public ValueDomainSubset<?, ?> getDomain(String name);

	/**
	 * Returns a dataset structure with the specified name if it exists.
	 * 
	 * @param name the name of the structure
	 * @return the structure or null if none exists.
	 */
	public DataSetMetadata getStructure(String name);

	/**
	 * Returns a data point ruleset with the specified name if it exists.
	 * 
	 * @param name the name of the ruleset
	 * @return the ruleset or null if none exists.
	 */
	public DataPointRuleSet getDataPointRuleset(String name);

	/**
	 * Returns a hierarchical ruleset with the specified name if it exists.
	 * 
	 * @param name the name of the ruleset
	 * @return the ruleset or null if none exists.
	 */
	public HierarchicalRuleSet<?, ?, ?> getHierarchyRuleset(String name);

	/**
	 * Registers a new domain instance inside this repository if it is not.
	 * 
	 * @param name the name of the new domain
	 * @param domain the domain to define 
	 * 
	 * @throws VTLCastException if the domain already exists and it's not the same.
	 */
	public void defineDomain(String name, ValueDomainSubset<?, ?> domain);
	
	/**
	 * Returns a {@link Variable} referred by an alias defined in this TransformationScheme.
	 * 
	 * @param alias the alias of the variable
	 * @return a {@link Variable} instance.
	 * @throws VTLUnboundAliasException if the alias is not defined.
	 */
	public <S extends ValueDomainSubset<S, D>, D extends ValueDomain> Variable<S, D> getVariable(String alias, ValueDomainSubset<S, D> domain);

	public <S extends ValueDomainSubset<S, D>, D extends ValueDomain> Variable<S, D> getDefaultVariable(ValueDomainSubset<S, D> domain);
	
	public TransformationScheme getTransformationScheme(String alias);
	
	/**
	 * Initialize this {@link MetadataRepository}.
	 * 
	 * This method should be always called once per instance, before attempting any other operation.
	 * 
	 * @param params optional initialization parameters
	 * @return this instance
	 */
	public default MetadataRepository init(Object... params)
	{
		return this;
	}
}
