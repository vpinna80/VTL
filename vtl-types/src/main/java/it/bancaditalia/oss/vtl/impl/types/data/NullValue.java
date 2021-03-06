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
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

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
	public static <T extends NullValue<T, R, S, D>, R extends Comparable<?> & Serializable, S extends ValueDomainSubset<S, D>, D extends ValueDomain> T instance(S domain)
	{
		return (T) INSTANCES.computeIfAbsent(domain, d -> new NullValue<>((S) d));
	}

	public static <T extends NullValue<T, R, S, D>, C extends DataStructureComponent<?, S, D>, R extends Comparable<?> & Serializable, S extends ValueDomainSubset<S, D>, D extends ValueDomain> T instanceFrom(C component)
	{
		return instance(component.getDomain());
	}

	@Override
	public String toString()
	{
		return "null";
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o) throws VTLNullCompareException
	{
		throw new VTLNullCompareException();
	}
}
