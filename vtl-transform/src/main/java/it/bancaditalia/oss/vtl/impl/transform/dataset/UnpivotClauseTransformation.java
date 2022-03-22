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
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.stream.Collectors.toSet;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLSyntaxException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class UnpivotClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(UnpivotClauseTransformation.class);
	private final String identifierName;
	private final String measureName;

	private static String sanitize(String string)
	{
		return string.replaceAll("^\"(.*)\"$", "$1");
	}
	
	public UnpivotClauseTransformation(String identifierName, String measureName)
	{
		this.identifierName = sanitize(identifierName);
		this.measureName = sanitize(measureName);
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) getThisValue(scheme);
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);
		
		Set<DataStructureComponent<Identifier, ?, ?>> oldIdentifiers = dataset.getComponents(Identifier.class);
		Set<DataStructureComponent<Measure,?,?>> oldMeasures = dataset.getComponents(Measure.class);
		DataStructureComponent<Identifier, EntireStringDomainSubset, StringDomain> newID = metadata.getComponent(identifierName, Identifier.class, STRINGDS).get();
		DataStructureComponent<Measure, ?, ?> newMeasure = metadata.getComponent(measureName, Measure.class).get();

		return new FunctionDataSet<>(metadata, ds -> ds.stream()
			.map(dp -> Utils.getStream(oldMeasures)
					.map(toEntryWithValue(m -> dp.get(m)))
					.filter(entryByValue(v -> !(v instanceof NullValue)))
					.map(e -> new DataPointBuilder(dp.getValues(oldIdentifiers))
							.add(newMeasure, e.getValue())
							.add(newID, StringValue.of(e.getKey().getName()))
							.build(getLineage(), metadata))
			).collect(concatenating(Utils.ORDERED)), dataset);
	}
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata value = getThisMetadata(scheme);

		if (!(value instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(value, DataSetMetadata.class);

		DataSetMetadata dataset = (DataSetMetadata) value;
		
		Set<? extends ValueDomainSubset<?, ?>> domains = dataset.getComponents(Measure.class).stream()
			.map(DataStructureComponent::getDomain)
			.distinct()
			.collect(toSet());

		if (domains.size() != 1)
			throw new VTLSyntaxException("For unpivot, all measures must be defined on the same domain, but " + domains + " were found.");
		
		ValueDomainSubset<?, ?> domain = domains.iterator().next();

		DataStructureComponent<Identifier, EntireStringDomainSubset, StringDomain> newIdentifier = new DataStructureComponentImpl<>(identifierName, Identifier.class, STRINGDS);
		DataStructureComponent<?, ?, ?> newMeasure = DataStructureComponentImpl.of(measureName, Measure.class, domain);

		return new DataStructureBuilder(dataset.getComponents(Identifier.class))
				.addComponent(newIdentifier)
				.addComponent(newMeasure)
				.build();
	}

	@Override
	public String toString()
	{
		return "unpivot " + identifierName + ", " + measureName;
	}
	
	@Override
	protected Lineage computeLineage()
	{
		return LineageNode.of(toString());
	}
}
