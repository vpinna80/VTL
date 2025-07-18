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

/**
 * The immutable representation of a component of a dataset.
 * 
 * @author Valentino Pinna
 *
 * @param <R> the {@link Component}
 */
public interface DataStructureComponent<R extends Component> extends Serializable
{
	/**
	 * @return The alias of this component
	 */
	public VTLAlias getAlias();
	
	/**
	 * @return The role of this {@link DataStructureComponent}.
	 */
	public Class<R> getRole(); 

	/**
	 * Checks if this {@link DataStructureComponent} has the specified role.
	 * 
	 * @param role the role
	 * @return true if this {@link DataStructureComponent} has the specified role.
	 */
	public default boolean is(Class<? extends Component> role)
	{
		return role.isAssignableFrom(getRole());
	}
	
	/**
	 * Convencience method that narrows the role of this {@link DataStructureComponent} to the specified role if possible.
	 * 
	 * @param <R2> The narrowed role 
	 * @param role the role to narrow to
	 * @return this component with the narrowed role.
	 * @throws ClassCastException if the role cannot be narrowed.
	 */
	@SuppressWarnings("unchecked")
	public default <R2 extends Component> DataStructureComponent<R2> asRole(Class<R2> role)
	{
		if (is(role))
			// safe
			return (DataStructureComponent<R2>) this;
		else
			throw new ClassCastException("In component " + this + ", cannot cast " + getRole().getSimpleName() + " to " + role.getSimpleName());
	}
	
	public default int defaultHashCode()
	{
		int prime = 31;
		int result = prime + getAlias().hashCode();
		result = prime * result + getRole().hashCode();
		return result;
	}
}
