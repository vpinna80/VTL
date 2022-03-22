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
package it.bancaditalia.oss.vtl.impl.types.domain;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DurationHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;

/**
 * Must be in period-length order 
 */
public class EntireDurationDomainSubset implements DurationDomainSubset<EntireDurationDomainSubset>, Serializable
{
	private static final long serialVersionUID = 1L;
	public static final EntireDurationDomainSubset INSTANCE = new EntireDurationDomainSubset();
	
	private EntireDurationDomainSubset()
	{

	}
	
	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof NullDomain || other instanceof DurationDomain;
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return other instanceof DurationDomain;
	}

	@Override
	public String getVarName()
	{
		return "duration_var";
	}

	@Override
	public ScalarValue<?, ?, EntireDurationDomainSubset, DurationDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof NullValue)
			return NullValue.instance(INSTANCE);
		else if (value instanceof DurationValue)
			return DurationValue.of((DurationHolder) value.get());
		throw new UnsupportedOperationException("Cast to duration domain");
	}
	
	@Override
	public String toString()
	{
		return "Duration";
	}
	
	@Override
	public DurationDomain getParentDomain()
	{
		return null;
	}
	
	@Override
	public int hashCode()
	{
		return getClass().hashCode();
	}
	
	@Override
	public boolean equals(Object obj)
	{
		return obj != null && obj.getClass() == getClass();
	}
	
	@Override
	public ScalarValue<?, ?, EntireDurationDomainSubset, DurationDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
}