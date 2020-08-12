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
package it.bancaditalia.oss.vtl.model.data;

import java.io.Serializable;
import java.util.function.Supplier;

public interface ScalarValue<R extends Comparable<?> & Serializable, S extends ValueDomainSubset<? extends D>, D extends ValueDomain> 
		extends VTLValue, Comparable<ScalarValue<?, ? extends ValueDomainSubset<?>, ? extends ValueDomain>>, Supplier<R>
{
	@FunctionalInterface
	public interface VTLScalarValueMetadata<S extends ValueDomainSubset<?>> extends VTLValueMetadata
	{
		public S getDomain();
	}
	
	public static int compare(ScalarValue<?, ? extends ValueDomainSubset<? extends ValueDomain>, ? extends ValueDomain> v1, 
			ScalarValue<?, ? extends ValueDomainSubset<?>, ? extends ValueDomain> v2)
	{
		return v1.compareTo(v2);
	}
	
	@Override
	public int compareTo(ScalarValue<?, ? extends ValueDomainSubset<?>, ? extends ValueDomain> o);
	
	@Override
	public VTLScalarValueMetadata<? extends ValueDomainSubset<? extends ValueDomain>> getMetadata();
	
	public S getDomain();
}
