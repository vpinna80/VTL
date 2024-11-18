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
package it.bancaditalia.oss.vtl.impl.transform;

import static it.bancaditalia.oss.vtl.impl.transform.GroupingClause.GroupingMode.GROUP_ALL;
import static it.bancaditalia.oss.vtl.impl.transform.GroupingClause.GroupingMode.GROUP_EXCEPT;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class GroupingClause implements Serializable
{
	private static final long serialVersionUID = 1L;

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

	private final GroupingMode mode;
	private final VTLAlias[] fields;
	private final Frequency frequency;

	public GroupingClause(GroupingMode mode, List<VTLAlias> fields, StringValue<?, ?> frequency)
	{
		this.mode = mode;
		this.fields = coalesce(fields, emptyList()).toArray(VTLAlias[]::new);
		this.frequency = frequency == null ? null : Frequency.valueOf(frequency.get());
	}

	public GroupingMode getMode()
	{
		return mode;
	}

	public VTLAlias[] getFields()
	{
		return fields;
	}
	
	public Frequency getFrequency()
	{
		return frequency;
	}
	
	public Set<DataStructureComponent<Identifier, ?, ?>> getGroupingComponents(DataSetMetadata dataset)
	{
		if (mode == GROUP_ALL)
			return dataset.getIDs();
		
		Set<DataStructureComponent<Identifier, ?, ?>> groupComps = Arrays.stream(fields)
				.peek(n -> { if (dataset.getComponent(n).isEmpty()) throw new VTLMissingComponentsException(n, dataset); })
				.map(dataset::getComponent)
				.map(Optional::get)
				.peek(component -> { if (!component.is(Identifier.class)) throw new VTLIncompatibleRolesException("aggregation group by", component, Identifier.class); })
				.map(component -> component.asRole(Identifier.class))
				.collect(toSet());
		
		if (mode == GROUP_EXCEPT)
		{
			Set<DataStructureComponent<Identifier, ?, ?>> exceptComps = new HashSet<>(dataset.getIDs());
			exceptComps.removeAll(groupComps);
			return exceptComps;
		}
		else
			return groupComps;
	}
	
	@Override
	public String toString()
	{
		return Arrays.stream(fields).map(VTLAlias::toString).collect(joining(", ", mode + " ", frequency != null ? " time_agg( " + frequency + " )" : ""));
	}
}
