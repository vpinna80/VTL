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
package it.bancaditalia.oss.vtl.impl.meta.subsets;

import it.bancaditalia.oss.vtl.exceptions.VTLCastException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.operators.CriterionTransformation;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class IntegerDomainRangeSubset implements DescribedDomainSubset<IntegerDomainRangeSubset, IntegerDomain>, IntegerDomainSubset<IntegerDomainRangeSubset>
{
	private static final long serialVersionUID = 1L;

	private final String name;
	private final long minInclusive;
	private final long maxInclusive;
	private final IntegerDomainSubset<?> parent;
	private final Transformation range = new CriterionTransformation() {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean test(ScalarValue<?, ?, ?, ?> scalar)
		{
			return scalar instanceof NullValue ||
					scalar instanceof IntegerValue && minInclusive <= (Long) scalar.get() && (Long) scalar.get() <= maxInclusive;
		}
	};
	
 	public IntegerDomainRangeSubset(String name, long minInclusive, long maxInclusive, IntegerDomainSubset<?> parent)
	{
		this.name = name;
		this.minInclusive = minInclusive;
		this.maxInclusive = maxInclusive;
		this.parent = parent;
	}

	@Override
	public IntegerDomainSubset<?> getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, IntegerDomainRangeSubset, IntegerDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		if (value instanceof NullValue)
			return NullValue.instance(this);
		
		long casted = ((Number) parent.cast(value).get()).longValue();
		
		if (minInclusive <= casted && casted <= maxInclusive)
			return IntegerValue.of(casted, this);
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other == this;
	}

	@Override
	public boolean isComparableWith(ValueDomain other)
	{
		return parent.isComparableWith(other);  
	}

	@Override
	public String getVarName()
	{
		return name + "_var";
	}

	@Override
	public Transformation getCriterion()
	{
		return range;
	}
	
	@Override
	public ScalarValue<?, ?, IntegerDomainRangeSubset, IntegerDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
	
	@Override
	public String getName()
	{
		return name + "{" + minInclusive + "," + maxInclusive + "}";
	}
	
	@Override
	public String toString()
	{
		return name + ":" + parent.getName();
	}
}
