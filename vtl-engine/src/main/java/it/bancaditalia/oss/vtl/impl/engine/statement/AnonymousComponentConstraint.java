/**
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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import static it.bancaditalia.oss.vtl.impl.engine.statement.AnonymousComponentConstraint.QuantifierConstraints.ANY;
import static it.bancaditalia.oss.vtl.impl.engine.statement.AnonymousComponentConstraint.QuantifierConstraints.AT_LEAST_ONE;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class AnonymousComponentConstraint extends DataSetComponentConstraint
{
	private static final long serialVersionUID = 1L;

	private final Class<? extends Component> role;
	private final String domainName;
	private final QuantifierConstraints quantifier;
	
	public enum QuantifierConstraints 
	{
		MAX_ONE, AT_LEAST_ONE, ANY
	}

	public AnonymousComponentConstraint(Class<? extends Component> role, String domainName, QuantifierConstraints quantifier)
	{
		super(null);

		this.role = role;
		this.domainName = domainName;
		this.quantifier = quantifier;
	}

	@Override
	protected Optional<Set<? extends DataStructureComponent<?, ?, ?>>> matchStructure(DataSetMetadata structure, MetadataRepository repo)
	{
		Set<? extends DataStructureComponent<?, ?, ?>> matchedComponents;
		if (domainName != null)
			matchedComponents = structure.getComponents(role, repo.getDomain(domainName));
		else
			matchedComponents = structure.getComponents(role);
		
		switch (quantifier)
		{
			case ANY:
				return Optional.of(matchedComponents);
			case AT_LEAST_ONE:
				return matchedComponents.size() > 0 ? Optional.of(matchedComponents) : Optional.empty();
			case MAX_ONE:
				return matchedComponents.size() > 0 ? Optional.of(Collections.singleton(matchedComponents.iterator().next())) : Optional.empty();
			default:
				throw new IllegalStateException("Unknown quantifier");
		}
	}
	
	@Override
	public String toString()
	{
		return role.getSimpleName().toLowerCase() 
				+ (domainName != null ? "<" + domainName + ">" : "")
				+ " _ " + (quantifier == ANY ? "*" : quantifier == AT_LEAST_ONE ? "+" : "");
	}
}
