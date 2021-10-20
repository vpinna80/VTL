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

import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.DescribedDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public abstract class EntireDomainSubset<S extends EntireDomainSubset<S, D>, D extends ValueDomain> implements DescribedDomainSubset<S, D>
{
	private static final long serialVersionUID = 1L;

	private final D parentDomain;
	private final String varName;
	
	public static final Transformation ENTIRE = new Transformation() {
		
		private static final long serialVersionUID = 1L;

		@Override
		public VTLValueMetadata getMetadata(TransformationScheme scheme)
		{
			return BOOLEAN;
		}
		
		@Override
		public VTLValue eval(TransformationScheme scheme)
		{
			return TRUE;
		}
		
		@Override
		public boolean isTerminal()
		{
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Set<LeafTransformation> getTerminals()
		{
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Lineage getLineage()
		{
			throw new UnsupportedOperationException();
		}
	};
	
	public EntireDomainSubset(D parentDomain, String defaultVarName)
	{
		this.parentDomain = parentDomain;
		this.varName = defaultVarName;

		// check no cycles in subsetting
		ValueDomain parent = parentDomain;
		while (parent != null)
		{
			if (parent == this)
				throw new UnsupportedOperationException("New subset of domain would form a cycle with parent " + parent + ".");
			
			if (parent instanceof ValueDomainSubset)
				parent = ((ValueDomainSubset<?, ?>) parent).getParentDomain();
		}
	}

	@Override
	public Transformation getCriterion()
	{
		return ENTIRE;
	}

	@Override
	public D getParentDomain()
	{
		return parentDomain;
	}
	
	@Override
	public String getVarName()
	{
		return varName;
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
	
	@SuppressWarnings("unchecked")
	@Override
	public ScalarValue<?, ?, S, D> getDefaultValue()
	{
		return NullValue.instance((S) this);
	}
}
