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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion;
import it.bancaditalia.oss.vtl.util.Utils;

public class WindowClauseImpl implements WindowClause, Serializable
{
	private static final long serialVersionUID = 1L;

	private final Set<DataStructureComponent<Identifier, ?, ?>> partitioningIds;
	private final List<SortCriterion> sortCriteria;
	private final WindowCriterion windowCriterion;
	
	public WindowClauseImpl(Set<DataStructureComponent<Identifier, ?, ?>> partitioningIds, List<SortCriterion> sortCriteria, WindowCriterion windowCriterion)
	{
		this.partitioningIds = Utils.coalesce(partitioningIds, emptySet());
		this.sortCriteria = Utils.coalesce(sortCriteria, emptyList());
		this.windowCriterion = windowCriterion;
	}


	@Override
	public Set<DataStructureComponent<Identifier, ?, ?>> getPartitioningIds()
	{
		return partitioningIds;
	}

	@Override
	public List<SortCriterion> getSortCriteria()
	{
		return sortCriteria;
	}
	
	@Override
	public WindowCriterion getWindowCriterion()
	{
		return windowCriterion;
	}


	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((partitioningIds == null) ? 0 : partitioningIds.hashCode());
		result = prime * result + ((sortCriteria == null) ? 0 : sortCriteria.hashCode());
		result = prime * result + ((windowCriterion == null) ? 0 : windowCriterion.hashCode());
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
		WindowClauseImpl other = (WindowClauseImpl) obj;
		if (partitioningIds == null)
		{
			if (other.partitioningIds != null)
				return false;
		}
		else if (!partitioningIds.equals(other.partitioningIds))
			return false;
		if (sortCriteria == null)
		{
			if (other.sortCriteria != null)
				return false;
		}
		else if (!sortCriteria.equals(other.sortCriteria))
			return false;
		if (windowCriterion == null)
		{
			if (other.windowCriterion != null)
				return false;
		}
		else if (!windowCriterion.equals(other.windowCriterion))
			return false;
		return true;
	}
}
