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

import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.util.Objects.requireNonNull;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
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
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.Utils;

public class PivotClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(PivotClauseTransformation.class);
	private final VTLAlias identifierName;
	private final VTLAlias measureName;

	private transient DataStructureComponent<Measure, ?, ?> measure;
	private transient DataStructureComponent<Identifier, ?, ?> identifier;

	public PivotClauseTransformation(VTLAlias identifierName, VTLAlias measureName)
	{
		this.identifierName = requireNonNull(identifierName);
		this.measureName = requireNonNull(measureName);
		
		LOGGER.debug("Pivoting " + measureName + " over " + identifierName);
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata value = getThisMetadata(scheme);

		if (!(value instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(value, DataSetMetadata.class);

		DataSetMetadata dataset = (DataSetMetadata) value;

		identifier = dataset.getComponent(identifierName, Identifier.class)
				.orElseThrow(() -> new VTLMissingComponentsException(identifierName, dataset.getIDs()));
		if (!(identifier.getVariable().getDomain() instanceof StringEnumeratedDomainSubset))
			throw new VTLException("The pivot identifier " + identifier + " should be of a codelist type.");
		
		measure = dataset.getComponent(measureName, Measure.class)
				.orElseThrow(() -> new VTLMissingComponentsException(measureName, dataset.getMeasures()));

		return createPivotStructure(dataset.getIDs(), scheme);
	}

	private DataSetMetadata createPivotStructure(Set<DataStructureComponent<Identifier, ?, ?>> ids, TransformationScheme scheme)
	{
		DataStructureBuilder builder = new DataStructureBuilder();
		MetadataRepository repo = scheme.getRepository();
		ValueDomainSubset<?, ?> measureDomain = measure.getVariable().getDomain();

		for (CodeItem<?, ?, ?, StringDomain> codeItem: ((StringEnumeratedDomainSubset<?, ?>) identifier.getVariable().getDomain()).getCodeItems())
			builder.addComponent(repo.createTempVariable(VTLAliasImpl.of(codeItem.get().toString()), measureDomain).as(Measure.class));
		
		return builder.addComponents(ids)
				.removeComponent(identifier)
				.removeComponent(measure).build();
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet dataset = (DataSet) getThisValue(session);
		DataSetMetadata structure = createPivotStructure(dataset.getMetadata().getIDs(), session);
		Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(structure.getIDs());
		Map<Object, DataStructureComponent<Measure, ?, ?>>measureMap = structure.getMeasures().stream()
				.collect(toConcurrentMap(c -> c.getVariable().getAlias(), identity()));
		
		String lineageString = toString();
		
		SerCollector<DataPoint, ?, DataPoint> collector = mapping(dp -> {
				DataStructureComponent<Measure, ?, ?> name = measureMap.get(dp.get(identifier).get().toString());
				return new SimpleEntry<>(name, new SimpleEntry<>(dp.getLineage(), dp.get(measure)));
			}, collectingAndThen(entriesToMap(), map -> {
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dp = Utils.getStream(map)
					.map(Utils.keepingKey(Entry::getValue))
					.collect(entriesToMap());
				Lineage[] lineages = map.values().toArray(new Lineage[map.size()]);
				return new DataPointBuilder(dp).build(LineageNode.of(lineageString, lineages), new DataStructureBuilder(dp.keySet()).build());
			})); 
		
		return dataset.aggregate(structure, ids, collector, (dp, keys) -> new DataPointBuilder(keys.getValue())
				.addAll(dp)
				.build(LineageNode.of(this, keys.getKey()), structure));
	}

	@Override
	public String toString()
	{
		return "pivot " + identifierName + ", " + measureName;
	}
}
