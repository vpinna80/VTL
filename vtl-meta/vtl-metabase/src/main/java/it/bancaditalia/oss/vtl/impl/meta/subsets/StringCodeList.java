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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;

public class StringCodeList extends AbstractStringCodeList implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final Set<StringCodeItemImpl> items = new HashSet<>();
	
	public StringCodeList(StringDomainSubset<?> parent, String name, Set<? extends String> items)
	{
		super(parent, name, s -> new StringCodeList(parent, name, s));
		for (String item: items)
			this.items.add(new StringCodeItemImpl(item));
		setHashCode(31 + name.hashCode() + this.items.hashCode());
	}
	
	@Override
	public Set<StringCodeItemImpl> getCodeItems()
	{
		return items;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		return super.equals(obj) && getClass() == obj.getClass() && ((StringCodeList) obj).items.equals(items);
	}
}
