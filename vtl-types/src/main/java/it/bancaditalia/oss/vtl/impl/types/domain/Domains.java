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
package it.bancaditalia.oss.vtl.impl.types.domain;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;

public enum Domains implements ScalarValueMetadata<ValueDomainSubset<?>>
{
	BOOLEAN(new EntireBooleanDomainSubset()),
	INTEGER(new EntireIntegerDomainSubset()),
	STRING(new EntireStringDomainSubset()),
	NUMBER(new EntireNumberDomainSubset()),
	DATE(new EntireDateDomainSubset()),
	TIME(EntireTimeDomainSubset.getInstance()), 
	DURATION(new EntireDurationDomainSubset());

	private final ValueDomainSubset<?> valueDomain;
	
	Domains(ValueDomainSubset<?> valueDomain)
	{
		this.valueDomain = valueDomain;
	}
	
	public ValueDomainSubset<?> getDomain()
	{
		return valueDomain;
	}
	
	public boolean isAssignableFrom(Domains other)
	{
		return getDomain().isAssignableFrom(other.getDomain());
	}

	public boolean isAssignableFrom(ValueDomainSubset<?> other)
	{
		return getDomain().isAssignableFrom(other);
	}

	public static class UnknownDomainSubset implements ValueDomainSubset<ValueDomain> 
	{
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isAssignableFrom(ValueDomain other)
		{
			return false;
		}

		@Override
		public boolean isComparableWith(ValueDomain other)
		{
			return false;
		}

		@Override
		public Object getCriterion()
		{
			return null;
		}

		@Override
		public ValueDomain getParentDomain()
		{
			return null;
		}
		
		@Override
		public ScalarValue<?, ? extends ValueDomainSubset<ValueDomain>, ValueDomain> cast(ScalarValue<?, ?, ?> value)
		{
			throw new UnsupportedOperationException("Cast to unknown domain not supported.");
		}

		@Override
		public String getVarName()
		{
			throw new UnsupportedOperationException("No variable name for unknown domain.");
		}
	};

	public static final BooleanDomainSubset BOOLEANDS = new EntireBooleanDomainSubset();
	public static final IntegerDomainSubset INTEGERDS = new EntireIntegerDomainSubset();
	public static final StringDomainSubset STRINGDS = new EntireStringDomainSubset();
	public static final NumberDomainSubset<NumberDomain> NUMBERDS = new EntireNumberDomainSubset();
	public static final DateDomainSubset DATEDS = new EntireDateDomainSubset();
	public static final TimeDomainSubset<TimeDomain> TIMEDS = EntireTimeDomainSubset.getInstance();
	public static final DurationDomainSubset DURATIONDS = new EntireDurationDomainSubset();
	public static final ValueDomainSubset<? extends ValueDomain> UNKNOWNDS = new UnknownDomainSubset();
}
