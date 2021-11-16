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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
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
	
	public WindowClauseImpl(Set<DataStructureComponent<Identifier, ?, ?>> partitioningIds, List<? extends SortCriterion> sortCriteria, WindowCriterion windowCriterion)
	{
		this.partitioningIds = Utils.coalesce(partitioningIds, emptySet());
		this.sortCriteria = Utils.coalesce(sortCriteria, emptyList()).stream().map(SortCriterion.class::cast).collect(toList());
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
}
