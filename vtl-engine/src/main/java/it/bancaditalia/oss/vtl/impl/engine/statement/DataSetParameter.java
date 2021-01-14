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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import static java.util.stream.Collectors.joining;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class DataSetParameter extends Parameter
{
	private static final long serialVersionUID = 1L;
	
	private final List<DataSetComponentConstraint> constraints;
	
	public DataSetParameter(String name, List<DataSetComponentConstraint> constraints)
	{
		super(name);
		
		this.constraints = constraints;
	}

	@Override
	public boolean matches(VTLValueMetadata metadata)
	{
		MetadataRepository repo = ConfigurationManager.getDefault().getMetadataRepository();
		
		if (metadata instanceof DataSetMetadata)
		{
			DataSetMetadata datasetMeta = (DataSetMetadata) metadata;
			Set<DataStructureComponent<?, ?, ?>> componentsRemaining = new HashSet<>(datasetMeta);
			
			// remove components once they are matched by each constraint
			for (DataSetComponentConstraint constraint: constraints)
			{
				Optional<Set<? extends DataStructureComponent<?, ?, ?>>> matchedComponents = constraint
						.matchStructure(new DataStructureBuilder(componentsRemaining).build(), repo);
				if (!matchedComponents.isPresent())
					return false;
				
				componentsRemaining.removeAll(matchedComponents.get());
			}
			
			// the entire structure is matched if all components have been matched
			return componentsRemaining.isEmpty();
		}
		else 
			return metadata instanceof UnknownValueMetadata;
	}

	@Override
	public String getMetaString()
	{
		return constraints.stream().map(Object::toString).collect(joining(", ", "dataset{", "}"));
	}
	
	@Override
	public String toString()
	{
		return getName() + " " + getMetaString();
	}
}
