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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.exceptions.VTLNullCompareException;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class NullValue<S extends ValueDomainSubset<S, D>, D extends ValueDomain> extends BaseScalarValue<NullValue<S, D>, String, S, D>
{
	private static final long serialVersionUID = 1L;
	private static final Map<ValueDomainSubset<? extends ValueDomainSubset<?, ?>, ? extends ValueDomain>, NullValue<?, ?>> INSTANCES = new ConcurrentHashMap<>();
	
	private NullValue(S domain)
	{
		super(null, domain);
	}
	
	public static <S extends ValueDomainSubset<S, D>, D extends ValueDomain> NullValue<S, D> instance(S domain)
	{
		@SuppressWarnings("unchecked")
		NullValue<S, D> nullValue = (NullValue<S, D>) INSTANCES.computeIfAbsent(domain, n -> new NullValue<>(domain));
		return nullValue;
	}

	public static NullValue<?, ?> unqualifiedInstance(ValueDomainSubset<?, ?> domain)
	{
		@SuppressWarnings("rawtypes")
		NullValue<?, ?> nullValue = INSTANCES.computeIfAbsent(domain, n -> new NullValue<>((ValueDomainSubset) domain));
		return nullValue;
	}

	public static <K extends Component, S extends ValueDomainSubset<S, D>, D extends ValueDomain> NullValue<S, D> instanceFrom(DataStructureComponent<K, S, D> component)
	{
		return instance(component.getVariable().getDomain());
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
