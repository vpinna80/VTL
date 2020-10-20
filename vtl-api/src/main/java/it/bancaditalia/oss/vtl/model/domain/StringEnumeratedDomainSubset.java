/**
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

public interface StringEnumeratedDomainSubset extends EnumeratedDomainSubset<StringDomainSubset, StringDomain>, StringDomainSubset
{
	public interface StringCodeItem extends CodeItem<String, StringEnumeratedDomainSubset, StringDomain>
	{

	}
	
	@Override ScalarValue<?, ? extends StringEnumeratedDomainSubset, StringDomain> cast(ScalarValue<?, ?, ?> value);
	
	@Override
	public Set<StringCodeItem> getCodeItems();
	
	public StringEnumeratedDomainSubset trim();

	public StringEnumeratedDomainSubset ltrim();

	public StringEnumeratedDomainSubset rtrim();

	public StringEnumeratedDomainSubset ucase();
	
	public StringEnumeratedDomainSubset lcase();
}
