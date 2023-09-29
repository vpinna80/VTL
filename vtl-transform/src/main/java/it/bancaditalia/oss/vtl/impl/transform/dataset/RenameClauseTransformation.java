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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class RenameClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	
	private final Map<String, String> renames;
	
	public RenameClauseTransformation(Map<String, String> renames)
	{
		this.renames = renames.entrySet().stream()
				.map(keepingKey(DataStructureComponent::normalizeAlias))
				.map(keepingValue(DataStructureComponent::normalizeAlias))
				.collect(entriesToMap());
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet operand = (DataSet) getThisValue(session);
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(session);
		DataSetMetadata oldStructure = operand.getMetadata();
		
		Map<String, ? extends DataStructureComponent<?, ?, ?>> oldComponents = renames.keySet().stream()
				.collect(toMap(name -> name, name -> oldStructure.getComponent(name).get()));

		Map<String, ? extends DataStructureComponent<?, ?, ?>> newComponents = renames.values().stream()
				.collect(toMap(name -> name, name -> metadata.getComponent(name).get()));

		return new FunctionDataSet<>(metadata, ds -> ds.stream()
				.map(dp -> new DataPointBuilder(dp)
						.addAll(renames.entrySet().stream()
								.map(splitting((oldName, newName) -> new SimpleEntry<>(newComponents.get(newName), 
										dp.get(oldComponents.get(oldName)))))
								.collect(entriesToMap()))
						.delete(oldComponents.values())
						.build(LineageNode.of(this, dp.getLineage()), metadata)
				), operand);
	}

	@Override
	public DataSetMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata operand = getThisMetadata(session);
		
		if (!(operand instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(operand, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) operand;
		DataSetMetadata accumulator = dataset;
		
		for (Entry<String, String> rename: renames.entrySet())
		{
			String alias = rename.getKey();
			DataStructureComponent<?, ?, ?> componentFrom = dataset.getComponent(alias)
					.orElseThrow(() -> new VTLMissingComponentsException(alias, dataset));
			
			if (accumulator.contains(rename.getValue()))
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
				.collect(joining(", ", "rename ", ""));
	}
}
