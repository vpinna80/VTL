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

import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.normalizeAlias;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.Utils;

public class PivotClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(PivotClauseTransformation.class);
	private final String identifierName;
	private final String measureName;

	private transient DataStructureComponent<Measure, ?, ?> measure;
	private transient DataStructureComponent<Identifier, ? extends StringEnumeratedDomainSubset<?, ?, ?>, StringDomain> identifier;

	public PivotClauseTransformation(String identifierName, String measureName)
	{
		this.identifierName = normalizeAlias(identifierName);
		this.measureName = normalizeAlias(measureName);
		
		LOGGER.debug("Pivoting " + measureName + " over " + identifierName);
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata value = getThisMetadata(session);

		if (!(value instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(value, DataSetMetadata.class);

		DataSetMetadata dataset = (DataSetMetadata) value;

		DataStructureComponent<Identifier, ?, ?> tempIdentifier = dataset.getComponent(identifierName, Identifier.class)
				.orElseThrow(() -> new VTLMissingComponentsException(identifierName, dataset.getComponents(Identifier.class)));
		if (!(tempIdentifier.getDomain() instanceof StringEnumeratedDomainSubset))
			throw new VTLException("pivot: " + tempIdentifier.getName() + " is of type " + tempIdentifier.getDomain() + " but should be of a StringEnumeratedDomainSubset.");
		identifier = tempIdentifier.asDomain((StringEnumeratedDomainSubset<?, ?, ?>) tempIdentifier.getDomain());
		
		measure = dataset.getComponent(measureName, Measure.class)
				.orElseThrow(() -> new VTLMissingComponentsException(measureName, dataset.getComponents(Measure.class)));

		return Utils.getStream(((StringEnumeratedDomainSubset<?, ?, ?>) identifier.getDomain()).getCodeItems())
				.map(i -> DataStructureComponentImpl.of(i.get().toString(), Measure.class, measure.getDomain()))
				.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge)
				.addComponents(dataset.getComponents(Identifier.class))
				.removeComponent(identifier)
				.removeComponent(measure).build();
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet dataset = (DataSet) getThisValue(session);
		DataSetMetadata structure = dataset.getMetadata().pivot(identifier, measure);
		Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(structure.getComponents(Identifier.class));
		String lineageString = toString();
		
		SerCollector<DataPoint, ?, DataPoint> collector = mapping(dp -> {
				DataStructureComponent<Measure, ?, ?> name = DataStructureComponentImpl.of(dp.get(identifier).get().toString(), Measure.class, measure.getDomain());
				return new SimpleEntry<>(name, new SimpleEntry<>(dp.getLineage(), dp.get(measure)));
			}, collectingAndThen(entriesToMap(), map -> {
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dp = Utils.getStream(map)
					.map(Utils.keepingKey(Entry::getValue))
					.collect(entriesToMap());
				Lineage[] lineages = map.values().toArray(new Lineage[map.size()]);
				return new DataPointBuilder(dp).build(LineageNode.of(lineageString, lineages), new DataStructureBuilder(dp.keySet()).build());
			})); 
		
		return dataset.aggr(structure, ids, collector, (dp, keys) -> new DataPointBuilder(keys)
				.addAll(dp)
				.build(LineageNode.of(this, dp.getLineage()), structure));
	}

	@Override
	public String toString()
	{
		return "pivot " + identifierName + ", " + measureName;
	}
}
