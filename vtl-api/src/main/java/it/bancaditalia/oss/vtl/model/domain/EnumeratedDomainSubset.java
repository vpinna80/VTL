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
package it.bancaditalia.oss.vtl.model.domain;

import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

/**
 * The representation of an enumerated domain subset (finite enumeration).
 * 
 * @author Valentino Pinna
 *
 * @param <S> the enumerated domain subset
 * @param <D> the base domain
 * @param <C> the code
 */
public interface EnumeratedDomainSubset<S extends EnumeratedDomainSubset<S, D>, D extends ValueDomain> 
	extends ValueDomainSubset<S, D>
{
	/**
	 * @return the domain name.
	 */
	public String getName();
	
	/**
	 * The returned set should not be altered in any way.
	 * 
	 * @return the set of all the code items in this domain.
	 */
	public Set<CodeItem<?, ?, S, D>> getCodeItems();

	@Override ScalarValue<?, ?, S, D> cast(ScalarValue<?, ?, ?, ?> value);
}
