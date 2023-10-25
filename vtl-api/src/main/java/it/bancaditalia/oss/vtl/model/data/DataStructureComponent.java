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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

/**
 * The immutable representation of a component of a dataset.
 * 
 * @author Valentino Pinna
 *
 * @param <R> the {@link ComponentRole}
 * @param <S> the {@link ValueDomainSubset}
 * @param <D> the {@link ValueDomain}
 */
public interface DataStructureComponent<R extends ComponentRole, S extends ValueDomainSubset<S, D>, D extends ValueDomain> extends Serializable
{
	public static int byName(DataStructureComponent<?, ?, ?> c1, DataStructureComponent<?, ?, ?> c2)
	{
		return c1.getName().compareTo(c2.getName());
	}

	public static int byNameAndRole(DataStructureComponent<?, ?, ?> c1, DataStructureComponent<?, ?, ?> c2)
	{
		if (c1.is(Attribute.class) && !c2.is(Attribute.class))
			return 1;
		else if (c1.is(Identifier.class) && !c2.is(Identifier.class))
			return -1;
		else if (c1.is(Measure.class) && c2.is(Identifier.class))
			return 1;
		else if (c1.is(Measure.class) && c2.is(Attribute.class))
			return -1;
	
		String n1 = c1.getName(), n2 = c2.getName();
		Pattern pattern = Pattern.compile("^(.+?)(\\d+)$");
		Matcher m1 = pattern.matcher(n1), m2 = pattern.matcher(n2);
		if (m1.find() && m2.find() && m1.group(1).equals(m2.group(1)))
			return Integer.compare(Integer.parseInt(m1.group(2)), Integer.parseInt(m2.group(2)));
		else
			return n1.compareTo(n2);
	}

	public static String normalizeAlias(String alias)
	{
		if (alias.matches("'.*'"))
			return alias.replaceAll("'(.*)'", "$1");
		else
			return alias.toLowerCase();
	}

	/**
	 * @return The dataset variable for this {@link DataStructureComponent}.
	 */
	public Variable getVariable();
	
	/**
	 * @return The domain subset of this {@link DataStructureComponent}.
	 */
	public S getDomain();
	
	/**
	 * Creates a new component by renaming this {@link DataStructureComponent}.
	 *  
	 * @param name The name to assign to the new component
	 * @return the new component.
	 */
	public DataStructureComponent<R, S, D> rename(String name);
	
	/**
	 * @return The role of this {@link DataStructureComponent}.
	 */
	public Class<R> getRole(); 
	
	/**
	 * Create a measure component with the same domain as this component and a default name. 
	 * @return The new component
	 */
	public DataStructureComponent<Measure, S, D> createMeasureFrom();

	/**
	 * @return The scalar value metadata of this {@link DataStructureComponent}.
	 */
	public default ScalarValueMetadata<S, D> getMetadata()
	{
		return this::getDomain;
	}
	
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
	public default <R2 extends ComponentRole> DataStructureComponent<R2, S, D> asRole(Class<R2> role)
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
	public default <S2 extends ValueDomainSubset<S2, D2>, D2 extends ValueDomain> DataStructureComponent<R, S2, D2> asDomain(ValueDomainSubset<S2, D2> domain)
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
	public default ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value)
	{
		return getDomain().cast(value);
	}
}
