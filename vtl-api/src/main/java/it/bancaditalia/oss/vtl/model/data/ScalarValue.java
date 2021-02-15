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
package it.bancaditalia.oss.vtl.model.data;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * An interface describing VTL scalar values, as defined by the specification.
 * 
 * @author Valentino Pinna
 *
 * @param <R> the Java class that is used to hold the effective value of this ScalarValue for computational 
 * 			purposes. Usually hidden as an implementation detail.
 * @param <S> the {@link ValueDomainSubset} of this value. 
 * @param <D> the basic {@link ValueDomain} as defined by VTL specification.
 */
public interface ScalarValue<R extends Comparable<?> & Serializable, S extends ValueDomainSubset<? extends D>, D extends ValueDomain> 
		extends VTLValue, Comparable<ScalarValue<?, ? extends ValueDomainSubset<?>, ? extends ValueDomain>>, Supplier<R>
{
	/**
	 * @return the {@link ValueDomainSubset} of this ScalarValue.
	 */
	public S getDomain();

	@Override
	public int compareTo(ScalarValue<?, ? extends ValueDomainSubset<?>, ? extends ValueDomain> o);
	
	@Override
	public ScalarValueMetadata<? extends ValueDomainSubset<? extends ValueDomain>> getMetadata();
}
