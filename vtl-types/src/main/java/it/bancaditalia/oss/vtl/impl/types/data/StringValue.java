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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.util.function.UnaryOperator;

import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;

public class StringValue<I extends StringValue<I, S>, S extends StringDomainSubset<S>> extends BaseScalarValue<I, String, S, StringDomain>
{
	private static final long serialVersionUID = 1L;
	private static final ScalarValue<?, ?, EntireStringDomainSubset, StringDomain> NULL_INSTANCE = NullValue.instance(STRINGDS);

	public static ScalarValue<?, ?, EntireStringDomainSubset, StringDomain> of(String value)
	{
		return value == null ? NULL_INSTANCE : new StringValue<>(value.replaceAll("^\"(.*)\"$", "$1"), STRINGDS);
	}

	public StringValue(String value, S domain)
	{
		super(value.replaceAll("^\"(.*)\"$", "$1"), domain);
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		return get().compareTo(STRINGDS.cast(o).get().toString());
	}
	
	@Override
	public String toString()
	{
		return '"' + get() + '"';
	}
	
	public StringValue<I, S> map(UnaryOperator<String> mapper)
	{
		return new StringValue<>(mapper.apply(get()), getDomain());
	}
}
