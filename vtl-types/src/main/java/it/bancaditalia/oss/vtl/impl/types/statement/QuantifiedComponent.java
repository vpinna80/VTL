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
package it.bancaditalia.oss.vtl.impl.types.statement;

import static it.bancaditalia.oss.vtl.impl.types.statement.QuantifiedComponent.Multiplicity.ANY;
import static it.bancaditalia.oss.vtl.impl.types.statement.QuantifiedComponent.Multiplicity.AT_LEAST_ONE;
import static it.bancaditalia.oss.vtl.model.data.Component.Role.COMPONENT;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

import java.util.List;

import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Component.Role;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class QuantifiedComponent extends ComponentParameterTypeImpl
{
	private static final long serialVersionUID = 1L;

	public enum Multiplicity
	{
		AT_LEAST_ONE, ANY;
	}

	private final Multiplicity quantifier;
	private final VTLAlias altName;
	
	public QuantifiedComponent(Role role, VTLAlias domainName, VTLAlias altName, Multiplicity quantifier)
	{
		super(coalesce(role, COMPONENT), domainName, null);
		
		this.quantifier = quantifier;
		this.altName = altName;
	}

	public boolean matches(DataStructureComponent<?, ?, ?> component, List<QuantifiedComponent> partialMatch, TransformationScheme scheme)
	{
		if (!getRole().roleClass().isAssignableFrom(component.getRole()))
			return false;
		else if (quantifier == null && partialMatch.contains(this))
			return false;
		else if (altName != null && !altName.equals(component.getVariable().getAlias()))
			return false;
		else if (getDomainName() != null && !scheme.getRepository().getDomain(getDomainName()).get().isAssignableFrom(component.getVariable().getDomain()))
			return false;
		else
			return true;
	}

	public boolean isSatisfiedBy(List<QuantifiedComponent> match)
	{
		long count = match.stream().filter(c -> this == c).count();

		if (quantifier == null && altName == null)
			return count <= 1;
		else if (altName != null)
			return count == 1;
		else if (quantifier == ANY)
			return true;
		else if (quantifier == AT_LEAST_ONE)
			return count >= 1;
		else
			throw new IllegalStateException();
	}
	
	@Override
	public String toString()
	{
		return (getRole() == null ? "" : getRole().roleClass().getSimpleName().toLowerCase() 
				+ (getDomainName() != null ? "<" + getDomainName() + ">" : "") + " ")
				+ coalesce(altName, "_")
				+ (quantifier != null ? quantifier == AT_LEAST_ONE ? "+" : "*" : "");
	}
}