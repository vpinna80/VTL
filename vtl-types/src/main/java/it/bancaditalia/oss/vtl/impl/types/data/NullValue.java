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
package it.bancaditalia.oss.vtl.impl.types.data;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLNullCompareException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class NullValue<T extends NullValue<T, R, S, D>, R extends Comparable<?> & Serializable, S extends ValueDomainSubset<S, D>, D extends ValueDomain> 
		extends BaseScalarValue<T, R, S, D>
{
	private static final long serialVersionUID = 1L;
	private static final Map<ValueDomainSubset<? extends ValueDomainSubset<?, ?>, ? extends ValueDomain>, NullValue<?, ?, ?, ?>> INSTANCES = new ConcurrentHashMap<>();
	
	private NullValue(S domain)
	{
		super(null, domain);
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends NullValue<T, R, S, D>, R extends Comparable<?> & Serializable, S extends ValueDomainSubset<S, D>, D extends ValueDomain, SS extends S> T instance(SS domain)
	{
		NullValue<?, ?, ?, ?> nullValue = (T) INSTANCES.get(domain);
		if (nullValue != null)
			return (T) nullValue;
		nullValue = new NullValue<>(domain);
		INSTANCES.put(domain, nullValue);
		return (T) nullValue;
	}

	public static <T extends NullValue<T, R, S, D>, K extends ComponentRole, R extends Comparable<?> & Serializable, S extends ValueDomainSubset<S, D>, D extends ValueDomain> T instanceFrom(DataStructureComponent<K, S, D> component)
	{
		return instance(component.getDomain());
	}

	@Override
	public String toString()
	{
		return "#NULL#";
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o) throws VTLNullCompareException
	{
		throw new VTLNullCompareException();
	}
}
