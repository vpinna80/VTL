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
package it.bancaditalia.oss.vtl.impl.transform.util;

import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion;

public class SortClause implements SortCriterion, Serializable
{
	private static final long serialVersionUID = 1L;

	private final DataSetComponent<?, ?, ?> component;
	private final SortingMethod method;

	public SortClause(DataSetComponent<?, ?, ?> component)
	{
		this(component, ASC);
	}
	
	public SortClause(DataSetComponent<?, ?, ?> component, SortingMethod method)
	{
		this.component = component;
		this.method = coalesce(method, ASC);
	}
	
	@Override
	public DataSetComponent<?, ?, ?> getComponent()
	{
		return component;
	}

	@Override
	public SortingMethod getMethod()
	{
		return method;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((component == null) ? 0 : component.hashCode());
		result = prime * result + ((method == null) ? 0 : method.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SortClause other = (SortClause) obj;
		if (component == null)
		{
			if (other.component != null)
				return false;
		}
		else if (!component.equals(other.component))
			return false;
		if (method != other.method)
			return false;
		return true;
	}
}