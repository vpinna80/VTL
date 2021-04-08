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
import it.bancaditalia.oss.vtl.model.data.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

/**
 * Representation of a finite enumerated subset of the VTL "String" domain (essentially, a code list).
 * 
 * @author Valentino Pinna
 */
public interface StringEnumeratedDomainSubset extends EnumeratedDomainSubset<StringEnumeratedDomainSubset, StringDomain>, StringDomainSubset<StringEnumeratedDomainSubset>
{
	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by trimming all leading and 
	 * trailing whitespace from each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public StringEnumeratedDomainSubset trim();

	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by trimming all leading 
	 * whitespace from each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public StringEnumeratedDomainSubset ltrim();

	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by trimming all trailing 
	 * whitespace from each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public StringEnumeratedDomainSubset rtrim();

	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by converting in upper 
	 * case each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public StringEnumeratedDomainSubset ucase();
	
	/**
	 * Creates a new {@link StringEnumeratedDomainSubset} by converting in lower
	 * case each code item of this {@link StringEnumeratedDomainSubset}.
	 *  
	 * @return the new domain.
	 */
	public StringEnumeratedDomainSubset lcase();

	/**
	 * A {@link CodeItem} having a String value that is allowed in a {@link StringEnumeratedDomainSubset}.
	 * 
	 * @author Valentino Pinna
	 */
	public interface StringCodeItem<I extends StringCodeItem<I>> extends CodeItem<I, String, StringEnumeratedDomainSubset, StringDomain>
	{

	}
	
	@Override
	ScalarValue<?, ?, StringEnumeratedDomainSubset, StringDomain> cast(ScalarValue<?, ?, ?, ?> value);
	
	@Override
	Set<? extends StringCodeItem<?>> getCodeItems();
}
