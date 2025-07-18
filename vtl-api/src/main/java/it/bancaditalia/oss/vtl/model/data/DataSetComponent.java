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
import java.util.Objects;

import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

/**
 * The immutable representation of a component of a dataset.
 * 
 * @author Valentino Pinna
 *
 * @param <R> the {@link Component}
 * @param <S> the {@link ValueDomainSubset}
 * @param <D> the {@link ValueDomain}
 */
public interface DataSetComponent<R extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> extends Serializable
{
	public static int byNameAndRole(DataSetComponent<?, ?, ?> c1, DataSetComponent<?, ?, ?> c2)
	{
		if (c1.is(Attribute.class) && !c2.is(Attribute.class))
			return 1;
		else if (c1.is(Identifier.class) && !c2.is(Identifier.class))
			return -1;
		else if (c1.is(Measure.class) && c2.is(Identifier.class))
			return 1;
		else if (c1.is(Measure.class) && c2.is(Attribute.class))
			return -1;

		return VTLAlias.byName().compare(c1.getAlias(), c2.getAlias());
	}

	/**
	 * @return The alias of this component
	 */
	public VTLAlias getAlias();
	
	/**
	 * @return The role of this {@link DataStructureComponent}.
	 */
	public Class<R> getRole(); 

	/**
	 * @return The dataset variable for this {@link DataSetComponent}.
	 */
	public ValueDomainSubset<S, D> getDomain();

	/**
	 * Create a new DataSetComponent as a copy of this component with a new alias.
	 * @param newAlias The new alias
	 * @return the new {@link DataSetComponent}
	 */
	public DataSetComponent<R, S, D> getRenamed(VTLAlias newAlias);
	
	/**
	 * Checks if this {@link DataSetComponent} has the specified role.
	 * 
	 * @param role the role
	 * @return true if this {@link DataSetComponent} has the specified role.
	 */
	public default boolean is(Class<? extends Component> role)
	{
		return role.isAssignableFrom(getRole());
	}
	
	/**
	 * Convencience method that narrows the role of this {@link DataSetComponent} to the specified role if possible.
	 * 
	 * @param <R2> The narrowed role 
	 * @param role the role to narrow to
	 * @return this component with the narrowed role.
	 * @throws ClassCastException if the role cannot be narrowed.
	 */
	@SuppressWarnings("unchecked")
	public default <R2 extends Component> DataSetComponent<R2, S, D> asRole(Class<R2> role)
	{
		if (is(role))
			// safe
			return (DataSetComponent<R2, S, D>) this;
		else
			throw new ClassCastException("In component " + this + ", cannot cast " + getRole().getSimpleName() + " to " + role.getSimpleName());
	}
	
	public default int defaultHashCode()
	{
		return Objects.hash(getAlias(), getRole(), getDomain());
	}
}
