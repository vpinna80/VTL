/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.util.stream.Collectors.joining;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class RenameClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private final Map<String, String> renames;
	
	public RenameClauseTransformation(Map<String, String> renames)
	{
		this.renames = renames;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet operand = (DataSet) getThisValue(session);
		VTLDataSetMetadata metadata = getMetadata(session);
		VTLDataSetMetadata oldStructure = operand.getDataStructure();
		
		Map<String, ? extends DataStructureComponent<?, ?, ?>> oldComponents = renames.keySet().stream()
				.collect(Collectors.toMap(name -> name, name -> oldStructure.getComponent(name).get()));

		Map<String, ? extends DataStructureComponent<?, ?, ?>> newComponents = renames.values().stream()
				.collect(Collectors.toMap(name -> name, name -> metadata.getComponent(name).get()));

		return new LightFDataSet<>(metadata, ds -> ds.stream()
				.map(dp -> new DataPointBuilder(dp)
						.addAll(renames.entrySet().stream()
								.map(splitting((oldName, newName) -> new SimpleEntry<>(newComponents.get(newName), 
										dp.get(oldComponents.get(oldName)))))
								.collect(Utils.entriesToMap()))
						.delete(oldComponents.values())
						.build(metadata)
				), operand);
	}

	@Override
	public VTLDataSetMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata operand = getThisMetadata(session);
		
		if (!(operand instanceof VTLDataSetMetadata))
			throw new VTLInvalidParameterException(operand, VTLDataSetMetadata.class);
		
		VTLDataSetMetadata dataset = (VTLDataSetMetadata) operand;
		VTLDataSetMetadata accumulator = dataset;
		
		for (Entry<String, String> rename: renames.entrySet())
		{
			DataStructureComponent<?, ?, ?> componentFrom = dataset.getComponent(rename.getKey())
					.orElseThrow(() -> new VTLMissingComponentsException(rename.getKey(), dataset));
			
			if (accumulator.containsComponent(rename.getValue()))
				throw new VTLException("rename from " + componentFrom + " cannot override existing component " + dataset.getComponent(rename.getValue()));
				
			accumulator = accumulator.rename(componentFrom, rename.getValue());
		}
		
		return accumulator;
	}
	
	@Override
	public String toString()
	{
		return renames.entrySet().stream()
				.map(e -> e.getKey() + " to " + e.getValue())
				.collect(joining(", ", "[rename ", "]"));
	}
}
