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

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

/**
 * Representation of a VTL Represented Variable, that provides a name and a domain subset.
 * 
 * @author Valentino Pinna
 * @param <S> The {@link ValueDomainSubset} type
 * @param <D> The {@link ValueDomain} type of the domain subset
 */
public interface Variable<S extends ValueDomainSubset<S, D>, D extends ValueDomain> extends ScalarValueMetadata<S, D>
{
	/**
	 * A Comparator to lexically sort Variables by their name.
	 * @see Comparator#compare(Object, Object)
	 *  
	 * @param v1 first variable
	 * @param v2 second variable
	 * @return a negative integer, zero, or a positive integer as the
     *         first argument is less than, equal to, or greater than the
     *         second
	 */
	public static int byName(Variable<?, ?> v1, Variable<?, ?> v2)
	{
		String n1 = v1.getAlias().getName(), n2 = v2.getAlias().getName();
		Pattern pattern = Pattern.compile("^(.+?)(\\d+)$");
		Matcher m1 = pattern.matcher(n1), m2 = pattern.matcher(n2);
		if (m1.find() && m2.find() && m1.group(1).equals(m2.group(1)))
			return Integer.compare(Integer.parseInt(m1.group(2)), Integer.parseInt(m2.group(2)));
		else
			return n1.compareTo(n2);
	}

	/**
	 * @return the {@link VTLAlias} of this {@link Variable}.
	 */
	public VTLAlias getAlias();

	/**
	 * Returns a {@link DataStructureComponent} referring this variable with the specified {@link Component} role.
	 * 
	 * @param <R> The component role type
	 * @param role The class of the component
	 * @return the component
	 */
	public <R extends Component> DataStructureComponent<R, S, D> as(Class<R> role);

	/**
	 * @return The domain subset of this {@link Variable}.
	 */
	public S getDomain();

	/**
	 * Casts a given value to the domain subset of this {@link Variable} if possible.
	 * 
	 * Equivalent to <code>getDomain().cast(value)</code>
	 * 
	 * @param value the value to cast
	 * @return the casted value.
	 * @throws VTLCastException if the value cannot be casted to the domain of this variable.
	 */
	public default ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value)
	{
		return getDomain().cast(value);
	}
	
	/**
	 * A default implementation to initialize the hashCode
	 * 
	 * @return A default variable hashCode.
	 */
	public default int defaultHashCode()
	{
		int prime = 31;
		int result = prime + getDomain().hashCode();
		result = prime * result + getAlias().hashCode();
		return result;
	}
}
