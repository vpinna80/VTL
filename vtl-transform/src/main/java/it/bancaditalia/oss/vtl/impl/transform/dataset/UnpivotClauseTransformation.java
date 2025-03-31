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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class UnpivotClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(UnpivotClauseTransformation.class);
	private final VTLAlias identifierName;
	private final VTLAlias measureName;

	public UnpivotClauseTransformation(VTLAlias identifierName, VTLAlias measureName)
	{
		this.identifierName = requireNonNull(identifierName);
		this.measureName = requireNonNull(measureName);
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) getThisValue(scheme);
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);
		
		DataStructureComponent<Identifier, ?, ?> newID = metadata.getComponent(identifierName, Identifier.class, STRINGDS).get();
		DataStructureComponent<Measure, ?, ?> newMeasure = metadata.getComponent(measureName, Measure.class).get();

		return dataset.flatmapKeepingKeys(metadata, lineageEnricher(this), dp -> {
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> measureVals = dp.getValues(Measure.class);
			return measureVals.entrySet().stream()
				.map(splitting((m, v) -> {
					Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> res = new HashMap<>();
					res.put(newID, StringValue.of(m.getVariable().getAlias().toString()));
					res.put(newMeasure, v);
					return res;
				}));
		});
	}
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata value = getThisMetadata(scheme);

		if (!(value.isDataSet()))
			throw new VTLInvalidParameterException(value, DataSetMetadata.class);

		DataSetMetadata dataset = (DataSetMetadata) value;

		Optional<DataStructureComponent<?, ?, ?>> maybeId = dataset.getComponent(identifierName);
		if (maybeId.isPresent())
			throw new VTLInvariantIdentifiersException("unpivot", singleton(maybeId.get().asRole(Identifier.class)));
		
		Set<? extends ValueDomainSubset<?, ?>> domains = dataset.getMeasures().stream()
			.map(DataStructureComponent::getVariable).map(Variable::getDomain)
			.distinct()
			.collect(toSet());

		if (domains.size() != 1)
			throw new VTLException("For unpivot, all measures must be defined on the same domain, but " + domains + " were found.");
		
		ValueDomainSubset<?, ?> domain = domains.iterator().next();
		MetadataRepository repo = scheme.getRepository();

		return new DataStructureBuilder(dataset.getIDs())
				.addComponent(repo.createTempVariable(identifierName, STRINGDS).as(Identifier.class))
				.addComponent(repo.createTempVariable(measureName, domain).as(Measure.class))
				.build();
	}

	@Override
	public String toString()
	{
		return "unpivot " + identifierName + ", " + measureName;
	}
}
