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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import static it.bancaditalia.oss.vtl.impl.engine.statement.DataSetComponentConstraint.QuantifierConstraints.MAX_ONE;
import static it.bancaditalia.oss.vtl.impl.engine.statement.DataSetComponentConstraint.QuantifierConstraints.ONE;
import static it.bancaditalia.oss.vtl.model.data.Component.Role.COMPONENT;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

import java.io.Serializable;
import java.util.List;

import it.bancaditalia.oss.vtl.model.data.Component.Role;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DataSetComponentConstraint implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final String name;
	private final Role role;
	private final VTLAlias domainName;
	private final QuantifierConstraints quantifier;
	
	public enum QuantifierConstraints 
	{
		ONE(""), MAX_ONE("_?"), AT_LEAST_ONE("_+"), ANY("_*");

		private final String repr;

		QuantifierConstraints(String repr)
		{
			this.repr = repr;
		}

		String getRepr()
		{
			return repr;
		}
	}
	
	public DataSetComponentConstraint(String name, Role role, VTLAlias domainName, QuantifierConstraints quantifier)
	{
		this.name = name;
		this.role = coalesce(role, COMPONENT);
		this.domainName = domainName;
		this.quantifier = coalesce(quantifier, ONE);
	}

	public String getName()
	{
		return name;
	}

	public boolean matches(DataStructureComponent<?, ?, ?> component, List<DataSetComponentConstraint> partialMatch, TransformationScheme scheme)
	{
		if (!role.roleClass().isAssignableFrom(component.getRole()))
			return false;
		else if ((quantifier == null || quantifier == MAX_ONE) && partialMatch.contains(this))
			return false;
		else if (name != null && !name.equals(component.getVariable().getAlias()))
			return false;
		else if (domainName != null && !scheme.getRepository().getDomain(domainName).isAssignableFrom(component.getVariable().getDomain()))
			return false;
		else
			return true;
	}

	public boolean isSatisfiedBy(List<DataSetComponentConstraint> match)
	{
		long count = match.stream().filter(c -> this == c).count();

		switch (quantifier)
		{
			case ANY: return true;
			case AT_LEAST_ONE: return count >= 1;
			case MAX_ONE: return count <= 1;
			case ONE: return count == 1;
			default: throw new IllegalStateException();
		}
	}
	
	@Override
	public String toString()
	{
		return (role != null ? role.roleClass().getSimpleName().toLowerCase() + (domainName != null ? "<" + domainName + ">" : "") + " " : "")
				+ (name != null ? name : "")
				+ (quantifier != ONE ? quantifier.getRepr() : "");
	}
}