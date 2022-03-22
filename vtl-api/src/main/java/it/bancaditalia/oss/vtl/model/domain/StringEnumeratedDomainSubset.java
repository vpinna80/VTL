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

import java.io.Serializable;

/**
 * Representation of a finite enumerated subset of the VTL "String" domain (essentially, a code list).
 * 
 * @author Valentino Pinna
 */
public interface StringEnumeratedDomainSubset<S extends StringEnumeratedDomainSubset<S, I, C, R>, I extends StringDomainSubset<I>, C extends StringCodeItem<C, R, S, I>, R extends Comparable<?> & Serializable> extends EnumeratedDomainSubset<S, I, StringDomain, C, R>, StringDomainSubset<S>
{
	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by trimming all leading and 
	 * trailing whitespace from each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public S trim();

	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by trimming all leading 
	 * whitespace from each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public S ltrim();

	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by trimming all trailing 
	 * whitespace from each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public S rtrim();

	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by converting in upper 
	 * case each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public S ucase();
	
	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by converting in lower
	 * case each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public S lcase();
}
