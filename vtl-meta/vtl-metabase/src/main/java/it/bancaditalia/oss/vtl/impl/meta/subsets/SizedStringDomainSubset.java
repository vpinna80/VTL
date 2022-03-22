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
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.operators.CriterionTransformation;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class SizedStringDomainSubset<T extends StringValue<T, SizedStringDomainSubset<T>>> implements StringDomainSubset<SizedStringDomainSubset<T>>, DescribedDomainSubset<SizedStringDomainSubset<T>, StringDomain>
{
	private static final long serialVersionUID = 1L;

	private final String name;
	private final StringDomainSubset<?> parent;
	private final int maxLen;
	private final Transformation lenChecker = new CriterionTransformation() {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean test(ScalarValue<?, ?, ?, ?> scalar)
		{
			return scalar instanceof StringValue && scalar.get().toString().length() <= maxLen;
		}
	};
	
	public SizedStringDomainSubset(String name, StringDomainSubset<? extends StringDomainSubset<?>> parent, int maxLen)
	{
		this.name = name;
		this.parent = parent;
		this.maxLen = maxLen;
	}

	@Override
	public StringDomainSubset<?> getParentDomain()
	{
		return parent;
	}

	@Override
	public ScalarValue<?, ?, SizedStringDomainSubset<T>, StringDomain> cast(ScalarValue<?, ?, ?, ?> value)
	{
		ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> casted = parent.cast(value);
		
		if (casted instanceof NullValue)
			return NullValue.instance(this);
		
		if (value.toString().length() <= maxLen)
			return new StringValue<>(value.toString(), this);
		else
			throw new VTLCastException(this, value);
	}

	@Override
	public boolean isAssignableFrom(ValueDomain other)
	{
		return other instanceof SizedStringDomainSubset && parent.isAssignableFrom(other);
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
		return lenChecker;
	}
	
	@Override
	public ScalarValue<?, ?, SizedStringDomainSubset<T>, StringDomain> getDefaultValue()
	{
		return NullValue.instance(this);
	}
	
	@Override
	public String toString()
	{
		return "string{" + maxLen + "}";
	}
}
