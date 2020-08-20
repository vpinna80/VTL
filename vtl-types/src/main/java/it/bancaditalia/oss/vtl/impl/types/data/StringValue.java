/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.types.data;

import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;

public class StringValue extends BaseScalarValue<String, StringDomainSubset, StringDomain>
{
	private static final long serialVersionUID = 1L;
	private static final VTLScalarValueMetadata<StringDomainSubset> META = () -> Domains.STRINGDS;

	public StringValue(String value)
	{
		super(value.replaceAll("^\"(.*)\"$", "$1"), Domains.STRINGDS);
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?> o)
	{
		return get().compareTo((String) Domains.STRINGDS.cast(o).get());
	}

	@Override
	public VTLScalarValueMetadata<StringDomainSubset> getMetadata()
	{
		return META;
	}
	
	@Override
	public String toString()
	{
		return '"' + super.toString() + '"';
	}
}
