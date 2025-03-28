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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineagesEnricher;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.util.Objects.requireNonNull;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;

public class PivotClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(PivotClauseTransformation.class);
	
	private final VTLAlias identifierName;
	private final VTLAlias measureName;

	public PivotClauseTransformation(VTLAlias identifierName, VTLAlias measureName)
	{
		this.identifierName = requireNonNull(identifierName);
		this.measureName = requireNonNull(measureName);
		
		LOGGER.debug("Pivoting " + measureName + " over " + identifierName);
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet dataset = (DataSet) getThisValue(session);
		DataSetMetadata structure = dataset.getMetadata();
		DataStructureComponent<Measure, ?, ?> pivotMeasure = structure.getComponent(measureName, Measure.class)
				.orElseThrow(() -> new VTLMissingComponentsException(measureName, structure.getMeasures()));
		DataStructureComponent<Identifier, ?, ?> pivotId = structure.getComponent(identifierName, Identifier.class)
				.orElseThrow(() -> new VTLMissingComponentsException(identifierName, structure.getIDs()));

		DataSetMetadata newStructure = createPivotStructure(pivotId, pivotMeasure, structure.getIDs(), session);
		Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(newStructure.getIDs());
		Map<VTLAlias, DataStructureComponent<Measure, ?, ?>> measureMap = newStructure.getMeasures().stream()
				.collect(toConcurrentMap(c -> c.getVariable().getAlias(), identity()));
		
		SerFunction<Collection<Lineage>, Lineage> enricher = lineagesEnricher(this);
		
		SerCollector<DataPoint, ?, DataPoint> collector = mapping(dp -> {
				DataStructureComponent<Measure, ?, ?> genMeasure = measureMap.get(VTLAliasImpl.of(true, "'" + dp.get(pivotId).get() + "'"));
				return new SimpleEntry<>(requireNonNull(genMeasure), dp);
			}, collectingAndThen(entriesToMap(), map -> {
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dp = new HashMap<>();
				List<Lineage> lineages = new ArrayList<>();
				for (DataStructureComponent<Measure, ?, ?> newMeasure: map.keySet())
				{
					dp.put(newMeasure, map.get(newMeasure).get(pivotMeasure));
					lineages.add(map.get(newMeasure).getLineage());
				}
				
				return new DataPointBuilder(dp, DONT_SYNC).build(enricher.apply(lineages), new DataStructureBuilder(dp.keySet()).build());
			})); 
		
		return dataset.aggregate(newStructure, ids, collector, (dp, lineages, keys) -> new DataPointBuilder(keys)
				.addAll(dp)
				.build(enricher.apply(lineages), newStructure));
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata value = getThisMetadata(scheme);

		if (!(value.isDataSet()))
			throw new VTLInvalidParameterException(value, DataSetMetadata.class);

		DataSetMetadata dataset = (DataSetMetadata) value;
		DataStructureComponent<Measure, ?, ?> pivotMeasure = dataset.getComponent(measureName, Measure.class)
				.orElseThrow(() -> new VTLMissingComponentsException(measureName, dataset.getMeasures()));
		DataStructureComponent<Identifier, ?, ?> pivotId = dataset.getComponent(identifierName, Identifier.class)
				.orElseThrow(() -> new VTLMissingComponentsException(identifierName, dataset.getIDs()));
		
		ValueDomainSubset<?, ?> idDomain = pivotId.getVariable().getDomain();
		if (!(idDomain instanceof EnumeratedDomainSubset))
			throw new VTLException("The pivot identifier " + pivotId.getVariable().getAlias() + " is defined on the '" + idDomain + " value domain, which is not enumerated.");
		
		return createPivotStructure(pivotId, pivotMeasure, dataset.getIDs(), scheme);
	}

	private DataSetMetadata createPivotStructure(DataStructureComponent<Identifier, ?, ?> pivotId, DataStructureComponent<Measure, ?, ?> pivotMeasure, Set<DataStructureComponent<Identifier, ?, ?>> ids, TransformationScheme scheme)
	{
		DataStructureBuilder builder = new DataStructureBuilder();
		MetadataRepository repo = scheme.getRepository();
		ValueDomainSubset<?, ?> measureDomain = pivotMeasure.getVariable().getDomain();

		for (CodeItem<?, ?, ?, ?> codeItem: ((EnumeratedDomainSubset<?, ?, ?>) pivotId.getVariable().getDomain()).getCodeItems())
			builder.addComponent(repo.createTempVariable(VTLAliasImpl.of(true, "'" + codeItem.get().toString() + "'"), measureDomain).as(Measure.class));
		
		return builder.addComponents(ids)
				.removeComponent(pivotId)
				.removeComponent(pivotMeasure).build();
	}

	@Override
	public String toString()
	{
		return "pivot " + identifierName + ", " + measureName;
	}
}
