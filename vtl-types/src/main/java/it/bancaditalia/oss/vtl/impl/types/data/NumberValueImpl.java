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

import java.util.Objects;

import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;

public abstract class NumberValueImpl<R extends Number & Comparable<? super R>, S extends NumberDomainSubset<D>, D extends NumberDomain> extends BaseScalarValue<R, S, D>
	implements NumberValue<R, S, D>
{
	private static final long serialVersionUID = 1L;

	public NumberValueImpl(R value, S domain)
	{
		super(Objects.requireNonNull(value), domain);
	}
	
	@Override
	public final int compareTo(ScalarValue<?, ?, ?> o)
	{
		if (o instanceof NumberValueImpl)
			return Double.valueOf(doubleValue()).compareTo(Double.valueOf(((NumberValueImpl<?, ?, ?>) o).doubleValue()));
		
		throw new VTLIncompatibleTypesException("comparison", getDomain(), o.getDomain());
	}
	
	@Override
	public double doubleValue()
	{
		return get().doubleValue();
	}
}
