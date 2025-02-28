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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DataSetParameter extends BaseParameter
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataSetParameter.class); 
	
	private final List<DataSetComponentConstraint> constraints;
	
	public DataSetParameter(VTLAlias name, List<DataSetComponentConstraint> constraints)
	{
		super(name);
		
		this.constraints = constraints;
	}

	@Override
	public boolean matches(TransformationScheme scheme, Transformation argument)
	{
		VTLValueMetadata metadata = argument.getMetadata(scheme);
		
		if (metadata.isDataSet())
		{
			LOGGER.info("Matching {} against parameter {}", metadata, this);
			List<List<DataSetComponentConstraint>> partialMatches = new ArrayList<>();
			partialMatches.add(new ArrayList<>());
			
			// for each component, find which components matches it, and create a "partial matching" tree 
			// of completed matches, in order to allow backtracking and consider all possible combinations
			for (DataStructureComponent<?, ?, ?> component: (DataSetMetadata) metadata)
			{
				List<List<DataSetComponentConstraint>> newMatches = new ArrayList<>();
				
				for (List<DataSetComponentConstraint> partialMatch: partialMatches)
					for (DataSetComponentConstraint constraint: constraints)
						if (constraint.matches(component, partialMatch, scheme))
						{
							List<DataSetComponentConstraint> newMatch = new ArrayList<>(partialMatch);
							newMatch.add(constraint);
							newMatches.add(newMatch);
						}

				if (newMatches.isEmpty())
				{
					LOGGER.info("Component {} does not match any constraint of parameter {}", component, this);
					return false;
				}
				else
					partialMatches = newMatches;
			}
			
			LOGGER.debug("Parameter {} has {} possible matches.", this, partialMatches.size());
			for (int i = 0; i < partialMatches.size(); i++)
			{
				List<DataSetComponentConstraint> match = partialMatches.get(i);
				boolean satisfied = true;
				
				for (DataSetComponentConstraint constraint: constraints)
					if (satisfied && !constraint.isSatisfiedBy(match))
					{
						LOGGER.debug("Solution {} dropped because constraints {} are not satisfied.", i, constraint);
						satisfied = false;
					}
				
				if (satisfied)
				{
					LOGGER.debug("Solution {} satisfies all constraints.", i);
					return true;
				}
			}
			
			return false;
		}
		else 
			return metadata instanceof UnknownValueMetadata;
	}

	@Override
	public String getDefinitionString()
	{
		return constraints.stream().map(Object::toString).collect(joining(", ", "dataset {", "}"));
	}
	
	@Override
	public String toString()
	{
		return getAlias() + " " + getDefinitionString();
	}
}
