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

import java.util.Set;

/**
 * The representation of an enumerated domain subset (finite enumeration).
 * 
 * @author Valentino Pinna
 *
 * @param <S> the domain subset
 * @param <D> the domain
 */
public interface EnumeratedDomainSubset<S extends ValueDomainSubset<D>, D extends ValueDomain> extends ValueDomainSubset<D>
{
	/**
	 * @return the domain name.
	 */
	public String getName();
	
	/**
	 * @return the original domain.
	 */
	public S getDomain();
	
	/**
	 * The returned set should not be altered in any way.
	 * 
	 * @return the set of all the code items in this domain.
	 */
	public Set<? extends CodeItem<? extends Comparable<?>, ? extends EnumeratedDomainSubset<S, D>, D>> getCodeItems();
	
	@Override ScalarValue<?, ? extends EnumeratedDomainSubset<S, D>, ? extends D> cast(ScalarValue<?, ?, ?> value);
}
