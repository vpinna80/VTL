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

import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class RenameClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	
	private final Map<VTLAlias, VTLAlias> renames;
	
	public RenameClauseTransformation(Map<VTLAlias, VTLAlias> renames)
	{
		this.renames = requireNonNull(renames);
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet operand = (DataSet) getThisValue(session);
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(session);
		DataSetMetadata oldStructure = operand.getMetadata();
		
		Map<VTLAlias, ? extends DataStructureComponent<?, ?, ?>> oldComponents = renames.keySet().stream()
				.collect(toMapWithValues(name -> oldStructure.getComponent(name)
						.orElseThrow(() -> new VTLMissingComponentsException(name, oldStructure))));

		Map<VTLAlias, ? extends DataStructureComponent<?, ?, ?>> newComponents = renames.values().stream()
				.collect(toMapWithValues(name -> metadata.getComponent(name)
						.orElseThrow(() -> new VTLMissingComponentsException(name, metadata))));

		if (oldComponents.values().stream().allMatch(c -> c.is(NonIdentifier.class)))
			return operand.mapKeepingKeys(metadata, lineageEnricher(this), dp -> {
					DataPoint renamed = dp;
					for (Entry<VTLAlias, VTLAlias> rename: renames.entrySet())
						renamed = renamed.rename(oldComponents.get(rename.getKey()), newComponents.get(rename.getValue()));
					return renamed;
				});
		else
		{
			SerUnaryOperator<Lineage> enricher = lineageEnricher(this);
			return new FunctionDataSet<>(metadata, ds -> ds.stream()
				.map(dp -> new DataPointBuilder(dp)
						.addAll(renames.entrySet().stream()
								.map(splitting((oldName, newName) -> new SimpleEntry<>(newComponents.get(newName), 
										dp.get(oldComponents.get(oldName)))))
								.collect(entriesToMap()))
						.delete(oldComponents.values())
						.build(enricher.apply(dp.getLineage()), metadata)
				), operand);
		}
	}

	@Override
	public DataSetMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata operand = getThisMetadata(session);
		
		if (!(operand.isDataSet()))
			throw new VTLInvalidParameterException(operand, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) operand;
		Map<VTLAlias, DataStructureComponent<?, ?, ?>> renamed = new HashMap<>();
		MetadataRepository repo = session.getRepository();
		
		for (DataStructureComponent<?, ?, ?> component: dataset)
		{
			VTLAlias oldName = component.getVariable().getAlias();
			VTLAlias newName = renames.get(oldName);
			if (newName != null)
				if (renamed.containsKey(newName))
					throw new VTLException("rename from " + oldName + " cannot override existing component " + dataset.getComponent(newName));
				else
					renamed.put(newName, repo.createTempVariable(newName, component.getVariable().getDomain()).as(component.getRole()));
			else 
				if (renamed.containsKey(oldName))
					throw new VTLException("A rename clause overrides existing component " + dataset.getComponent(oldName));
				else
					renamed.put(oldName, component);
		}
		
		for (Entry<VTLAlias, VTLAlias> rename: renames.entrySet())
			if (!renamed.containsKey(rename.getValue()))
				throw new VTLMissingComponentsException(rename.getKey(), dataset);
		
		return new DataStructureBuilder(renamed.values()).build();
	}
	
	@Override
	public String toString()
	{
		return renames.entrySet().stream()
				.map(e -> e.getKey() + " to " + e.getValue())
				.collect(joining(", ", "rename ", ""));
	}
}
