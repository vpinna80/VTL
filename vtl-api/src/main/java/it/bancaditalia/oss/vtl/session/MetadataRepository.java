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
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.DataStructureDefinition;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
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
	 * Returns a domain with the specified name if it exists.
	 * 
	 * @param name the name of the domain
	 * @return An optional containing the domain or an empty one if it's not defined.
	 */
	public Optional<ValueDomainSubset<?, ?>> getDomain(VTLAlias name);

	/**
	 * Returns the source definition for the dataset with the specified name if it's defined.
	 * 
	 * @param name the name of the dataset
	 * @return the source definition or null if it's not defined.
	 */
	public String getDataSource(VTLAlias name);

	/**
	 * Returns metadata for a given VTL alias if it's defined in the repository.
	 * 
	 * @param alias the alias of the data source to retrieve
	 * @return an optional containing metadata, or empty if none was defined.
	 */
	public Optional<VTLValueMetadata> getMetadata(VTLAlias alias);

	/**
	 * Returns the definition of a structure with the given VTL alias if it's defined in the repository.
	 * 
	 * @param alias the alias of the structure to retrieve
	 * @return an optional containing the structure defintion, or empty if none was defined.
	 */
	public Optional<DataStructureDefinition> getStructureDefinition(VTLAlias alias);

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
	public HierarchicalRuleSet getHierarchyRuleset(VTLAlias name);
	
	/**
	 * Returns a {@link Variable} referred by an alias if defined in this TransformationScheme.
	 * 
	 * @param alias the alias of the variable
	 * @return a optional containing the {@link Variable}, or empty if the variable was not defined.
	 */
	public Optional<Variable<?, ?>> getVariable(VTLAlias alias);

	/**
	 * @return A repository linked to this if there's one, or null
	 */
	public MetadataRepository getLinkedRepository();

	/**
	 * @return A set of aliases of each available {@link TransformationScheme} in this MetadataRepository.
	 */
	public Set<VTLAlias> getAvailableSchemeAliases();

	/**
	 * If this MetadataRepository supports storage of {@link TransformationScheme}s, try to retrieve one.
	 *   
	 * @param alias The alias of the TransformationScheme to retrieve
	 * @return The code associated to the given TransformationScheme
	 */
	public Optional<String> getTransformationScheme(VTLAlias alias);
}
