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
package it.bancaditalia.oss.vtl.model.data;

import java.io.Serializable;
import java.util.function.UnaryOperator;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;

/**
 * The immutable representation of a component of a dataset.
 * 
 * @author Valentino Pinna
 *
 * @param <R> the {@link ComponentRole}
 * @param <S> the {@link ValueDomainSubset}
 * @param <D> the {@link ValueDomain}
 */
public interface DataStructureComponent<R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> extends Serializable
{
	/**
	 * @return The dataset variable for this {@link DataStructureComponent}.
	 */
	public Variable getVariable();
	
	/**
	 * @return The domain subset of this {@link DataStructureComponent}.
	 */
	public S getDomain();
	
	/**
	 * @return The role of this {@link DataStructureComponent}.
	 */
	public Class<R> getRole(); 
	
	/**
	 * @return The name of this {@link DataStructureComponent}.
	 */
	public default String getName()
	{
		return getVariable().getName();
	}

	/**
	 * Creates a new component by renaming this {@link DataStructureComponent}.
	 *  
	 * @param name The name to assign to the new component
	 * @return the new component.
	 */
	public DataStructureComponent<R, S, D> rename(String name);

	/**
	 * Creates a new component by renaming this {@link DataStructureComponent}.
	 *  
	 * @param nameMapper a functions that returns the new name given the name of this {@link DataStructureComponent} 
	 * @return the new component.
	 */
	public default DataStructureComponent<R, S, D> rename(UnaryOperator<String> nameMapper)
	{
		return rename(nameMapper.apply(getName()));
	}

	/**
	 * Checks if this {@link DataStructureComponent} has the specified role.
	 * 
	 * @param role the role
	 * @return true if this {@link DataStructureComponent} has the specified role.
	 */
	public default boolean is(Class<? extends ComponentRole> role)
	{
		return role.isAssignableFrom(getRole());
	}
	
	/**
	 * Narrows the role of this {@link DataStructureComponent} to the specified role if possible.
	 * 
	 * @param role the role to narrow to
	 * @return this component with the narrowed role.
	 * @throws ClassCastException if the role cannot be narrowed.
	 */
	@SuppressWarnings("unchecked")
	public default <R2 extends ComponentRole> DataStructureComponent<R2, S, D> as(Class<R2> role)
	{
		if (is(role))
			// safe
			return (DataStructureComponent<R2, S, D>) this;
		else
			throw new ClassCastException("In component " + this + ", cannot cast " + getRole().getSimpleName() + " to " + role.getSimpleName());
	}
	
	/**
	 * Narrows the domain of this {@link DataStructureComponent} to the specified domain if possible.
	 * 
	 * @param domain the domain to narrow to
	 * @return this component with the narrowed domain.
	 * @throws ClassCastException if the domain cannot be narrowed.
	 */
	@SuppressWarnings("unchecked")
	public default <S2 extends ValueDomainSubset<D2>, D2 extends ValueDomain> DataStructureComponent<R, S2, D2> as(S2 domain)
	{
		if (domain.isAssignableFrom(getDomain()))
			// safe
			return (DataStructureComponent<R, S2, D2>) this;
		else
			throw new ClassCastException("Cannot cast component " + this + " from " + getDomain() + " to " + domain);
	}

	/**
	 * Casts a given value to the domain subset of this {@link DataStructureComponent} if possible.
	 * 
	 * @param value the value to cast
	 * @return the casted value.
	 * @throws VTLCastException if the value cannot be casted to the domain of this component.
	 */
	@SuppressWarnings("unchecked")
	public default ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value)
	{
		return (ScalarValue<?, ?, S, D>) getDomain().cast(value);
	}

	@Override
	public boolean equals(Object obj);
	
	@Override
	public int hashCode();
}
