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
package it.bancaditalia.oss.vtl.impl.meta.subsets;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class VariableImpl<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias alias;
	private final S domain;
	private final int hashCode;
	private final Map<Class<? extends Component>, DataStructureComponent<?, ?, ?>> components = new HashMap<>();
	
	public VariableImpl(VTLAlias name, S domain)
	{
		this.alias = requireNonNull(name);
		this.domain = requireNonNull(domain);

		int prime = 31;
		int result = 1;
		result = prime * result + domain.hashCode();
		result = prime * result + name.hashCode();
		hashCode = result;
		
		for (Class<? extends Component> role: Set.of(Identifier.class, Measure.class, Attribute.class, ViralAttribute.class))
			components.put(role, new DataStructureComponentImpl<>(role, this));
	}
	
	@SuppressWarnings("unchecked")
	public static <S extends ValueDomainSubset<S, D>, D extends ValueDomain> Variable<S, D> of(VTLAlias name, ValueDomainSubset<? super S, ? super D> domain)
	{
		return new VariableImpl<>(name, (S) domain);
	}

	@Override
	public VTLAlias getAlias()
	{
		return alias;
	}
	
	@Override
	public S getDomain()
	{
		return domain;
	}

	@Override
	public int hashCode()
	{
		return hashCode;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!(obj instanceof Variable))
			return false;
		
		Variable<?, ?> other = (Variable<?, ?>) obj;
		if (domain.equals(other.getDomain()) && alias.equals(other.getAlias()))
			return true;
		
		return false;
	}
	
	@Override
	public String toString()
	{
		return alias + "[" + domain + "]";
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <R extends Component> DataStructureComponent<R, S, D> as(Class<R> role)
	{
		return requireNonNull((DataStructureComponent<R, S, D>) components.get(role), "Unknown role: " + role.getSimpleName());
	}

	public Variable<S, D> getRenamed(VTLAlias newName)
	{
		return new VariableImpl<>(newName, domain);
	}
}
