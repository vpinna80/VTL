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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

/**
 * TODO Representation of a VTL variable
 * @author Valentino Pinna
 *
 */
public interface Variable<S extends ValueDomainSubset<S, D>, D extends ValueDomain> extends ScalarValueMetadata<S, D>
{
	public static int byName(Variable<?, ?> v1, Variable<?, ?> v2)
	{
		String n1 = v1.getName(), n2 = v2.getName();
		Pattern pattern = Pattern.compile("^(.+?)(\\d+)$");
		Matcher m1 = pattern.matcher(n1), m2 = pattern.matcher(n2);
		if (m1.find() && m2.find() && m1.group(1).equals(m2.group(1)))
			return Integer.compare(Integer.parseInt(m1.group(2)), Integer.parseInt(m2.group(2)));
		else
			return n1.compareTo(n2);
	}

	public String getName();
	
	/**
	 * @return The domain subset of this {@link DataStructureComponent}.
	 */
	public S getDomain();

	static String normalizeAlias(String alias)
	{
		if (alias.matches("'.*'"))
			return alias.replaceAll("'(.*)'", "$1");
		else
			return alias.toLowerCase();
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
