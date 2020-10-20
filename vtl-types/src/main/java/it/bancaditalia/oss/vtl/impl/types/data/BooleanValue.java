/**
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

import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;

public class BooleanValue extends BaseScalarValue<Boolean, BooleanDomainSubset, BooleanDomain>
{
	public static final BooleanValue FALSE = new BooleanValue(Boolean.FALSE);
	public static final BooleanValue TRUE = new BooleanValue(Boolean.TRUE);
	
	private static final long serialVersionUID = 1L;
	private static final ScalarValueMetadata<ValueDomainSubset<?>> META = Domains.BOOLEAN;

	private BooleanValue(Boolean value)
	{
		super(value, Domains.BOOLEANDS);
	}

	public static final BooleanValue of(boolean value)
	{
		return value ? TRUE : FALSE;
	}
	
	@Override
	public Boolean get()
	{
		return this == TRUE ? true : false;
	}

	@Override
	public int compareTo(ScalarValue<?, ?, ?> o)
	{
		throw new VTLIncompatibleTypesException("comparison", this, o);
	}
	
	@Override
	public ScalarValueMetadata<? extends ValueDomainSubset<?>> getMetadata()
	{
		return META;
	}
}
