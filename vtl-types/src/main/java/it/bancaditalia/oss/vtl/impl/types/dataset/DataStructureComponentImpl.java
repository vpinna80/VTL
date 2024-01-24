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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class DataStructureComponentImpl<R extends ComponentRole, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements DataStructureComponent<R, S, D>
{
	private static final long serialVersionUID = 1L;
	private final Variable<S, D> variable;
	private final Class<R> role;
	private transient int hashCode;
	
	public DataStructureComponentImpl(Class<R> role, Variable<S, D> variable)
	{
		this.role = role;
		this.variable = variable;
		this.hashCode = hashCodeInit();
	}
	
	public static <S extends ValueDomainSubset<S, D>, D extends ValueDomain, R extends ComponentRole> DataStructureComponent<R, S, D> of(Class<R> role, Variable<S, D> variable)
	{
		return new DataStructureComponentImpl<>(role, variable);
	}
	
	@SuppressWarnings("unchecked")
	public static <S extends ValueDomainSubset<S, D>, D extends ValueDomain, R extends ComponentRole> DataStructureComponent<R, S, D> of(String name, Class<R> role, ValueDomainSubset<S, D> domain)
	{
		return new DataStructureComponentImpl<>(role, new VariableImpl<>(name, (S) domain));
	}
	
	@SuppressWarnings("unchecked")
	public static <S extends ValueDomainSubset<S, D>, D extends ValueDomain, R extends ComponentRole> DataStructureComponent<R, S, D> of(Class<R> role, ValueDomainSubset<S, D> domain)
	{
		return new DataStructureComponentImpl<>(role, new VariableImpl<>((S) domain));
	}

	public DataStructureComponentImpl(Class<R> role, S domain)
	{
		this(role, new VariableImpl<>(domain));
	}

	@Override
	public Variable<S, D> getVariable()
	{
		return variable;
	}

	@Override
	public Class<R> getRole()
	{
		return role;
	}

	@Override
	public int hashCode()
	{
		return hashCode == 0 ? hashCode = hashCodeInit() : hashCode;
	}

	public int hashCodeInit()
	{
		final int prime = 31;
		int result = 1;
		result = prime * variable.hashCode();
		result = prime * result + ((role == null) ? 0 : role.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataStructureComponentImpl<?, ?, ?> other = (DataStructureComponentImpl<?, ?, ?>) obj;
		
		if (!role.equals(other.role))
			return false;
		if (!variable.equals(other.variable))
			return false;
		
		return true;
	}

	@Override
	public String toString()
	{
		return (is(Identifier.class) ? "$" : "") + (is(Attribute.class) ? "@" : "") + getVariable().getName() + "[" + getDomain() + "]";	
	}

	@Override
	public DataStructureComponent<?, ?, ?> rename(String newName)
	{
		return DataStructureComponentImpl.of(newName, role, getDomain());
	}
}
