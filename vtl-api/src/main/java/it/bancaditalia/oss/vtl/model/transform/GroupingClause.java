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
package it.bancaditalia.oss.vtl.model.transform;

import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;

/**
 * 
 */
public interface GroupingClause
{
	/**
	 * 
	 */
	public enum GroupingMode
	{
		GROUP_BY("group by"), GROUP_EXCEPT("group except"), GROUP_ALL("group all");

		private final String repr;

		GroupingMode(String repr)
		{
			this.repr = repr;
		}

		@Override
		public String toString()
		{
			return repr;
		}
	}

	/**
	 * @return a {@link GroupingMode} constant describing the grouping mode
	 */
	public GroupingMode getMode();

	/**
	 * 
	 * @return the list of names appearing after the group clause if specified in the VTL source. It may be null or empty.
	 */
	public VTLAlias[] getFields();

	/**
	 * 
	 * @return The duration value associated with the time_agg clause if specified in the VTL source. 
	 */
	public ScalarValue<?, ?, ? extends DurationDomainSubset<?> , DurationDomain> getFrequency();

	/**
	 * Projects this clause's mode and field names onto the given {@link DataSetStructure}, returning a set of 
	 * identifiers to be used to group the corresponding dataset.
	 *  
	 * @param dataset The structure of a dataset.
	 * @return The set of grouping identifiers.
	 */
	public Set<DataSetComponent<Identifier, ?, ?>> getGroupingComponents(DataSetStructure dataset);
}
