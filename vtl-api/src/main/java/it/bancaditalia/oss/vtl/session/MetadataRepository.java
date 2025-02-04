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

import java.util.Optional;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;

/**
 * A repository to contain and query all the defined domains.
 * 
 * @author Valentino Pinna
 */
public interface MetadataRepository
{
	/**
	 * Checks if a {@link ValueDomainSubset} with the specified name exists.
	 * 
	 * @param name the name of the domain to check
	 * @return true if a domain exists.
	 */
	public boolean isDomainDefined(VTLAlias name);
	
	/**
	 * Returns a domain with the specified name if it exists.
	 * 
	 * @param name the name of the domain
	 * @return the domain or null if none exists.
	 */
	public ValueDomainSubset<?, ?> getDomain(VTLAlias name);

	/**
	 * Returns the source definition for the dataset with the specified name if it's defined.
	 * 
	 * @param name the name of the dataset
	 * @return the source definition or null if it's not defined.
	 */
	public String getDataSource(VTLAlias name);

	/**
	 * Returns a metadata for a given alias if it is defined in the repository.
	 * 
	 * @param alias the alias of the data source to retrieve
	 * @return an optional containing the metadata, or empty if none was defined.
	 */
	public Optional<VTLValueMetadata> getMetadata(VTLAlias alias);

	/**
	 * Returns a data point ruleset with the specified name if it exists.
	 * 
	 * @param name the name of the ruleset
	 * @return the ruleset or null if none exists.
	 */
	public DataPointRuleSet getDataPointRuleset(VTLAlias name);

	/**
	 * Returns a hierarchical ruleset with the specified name if it exists.
	 * 
	 * @param name the name of the ruleset
	 * @return the ruleset or null if none exists.
	 */
	public HierarchicalRuleSet<?, ?, ?> getHierarchyRuleset(VTLAlias name);

	/**
	 * Registers a new domain instance inside this repository if it is not.
	 * 
	 * @param name the name of the new domain
	 * @param domain the domain to define 
	 * 
	 * @throws VTLCastException if the domain already exists and it's not the same.
	 */
	public void defineDomain(VTLAlias name, ValueDomainSubset<?, ?> domain);
	
	/**
	 * Returns a {@link Variable} referred by an alias defined in this TransformationScheme.
	 * 
	 * @param alias the alias of the variable
	 * @return a {@link Variable} instance.
	 * @throws VTLUnboundAliasException if the alias is not defined.
	 */
	public Variable<?, ?> getVariable(VTLAlias alias);

	/**
	 * Creates a temporary variable with provided alias and domain. An error is raised if a persistent variable with a different domain is already defined in metadata.
	 * 
	 * @param alias the alias of the variable
	 * @param alias the domain of the variable
	 * @return a {@link Variable} instance.
	 */
	public Variable<?, ?> createTempVariable(VTLAlias alias, ValueDomainSubset<?, ?> domain);
}
