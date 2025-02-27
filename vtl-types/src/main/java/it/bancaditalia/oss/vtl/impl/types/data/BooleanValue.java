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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;

public class BooleanValue<S extends BooleanDomainSubset<S>> extends BaseScalarValue<BooleanValue<S>, Boolean, S, BooleanDomain>
{
	private static final long serialVersionUID = 1L;
	public static final BooleanValue<EntireBooleanDomainSubset> FALSE = new BooleanValue<EntireBooleanDomainSubset>(Boolean.FALSE, BOOLEANDS);
	public static final BooleanValue<EntireBooleanDomainSubset> TRUE = new BooleanValue<EntireBooleanDomainSubset>(Boolean.TRUE, BOOLEANDS);
	public static final ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> NULL = NullValue.instance(BOOLEANDS);
	

	private BooleanValue(Boolean value, S domain)
	{
		super(value, domain);
	}

	public static final ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> of(Boolean value)
	{
		return value == null ? NULL : value ? TRUE : FALSE;
	}
	
	public static final ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> of(boolean value)
	{
		return value ? TRUE : FALSE;
	}
	
	public static final ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> not(ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> value)
	{
		return value.isNull() ? NULL : ((Boolean) value.get() ? FALSE : TRUE);
	}
	
	@Override
	public int compareTo(ScalarValue<?, ?, ?, ?> o)
	{
		if (o == this)
			return 0;
		else if (this == TRUE && o == FALSE)
			return 1;
		else if (this == FALSE && o == TRUE)
			return -1;
		else
			throw new VTLIncompatibleTypesException("comparison", this, o);
	}
}
