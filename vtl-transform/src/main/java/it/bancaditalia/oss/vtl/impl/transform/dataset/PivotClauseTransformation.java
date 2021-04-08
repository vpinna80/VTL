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

import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.toConcurrentMap;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class PivotClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(PivotClauseTransformation.class);
	private final String identifierName;
	private final String measureName;

	private DataStructureComponent<Measure, ?, ?> measure;
	private DataStructureComponent<Identifier, StringEnumeratedDomainSubset, StringDomain> identifier;

	private static String sanitize(String string)
	{
		return string.replaceAll("^\"(.*)\"$", "$1");
	}
	
	private static String sanitize(ScalarValue<?, ?, ?, ?> value)
	{
		return value.toString().replaceAll("^\"(.*)\"$", "$1");
	}
	
	public PivotClauseTransformation(String identifierName, String measureName)
	{
		this.identifierName = sanitize(identifierName);
		this.measureName = sanitize(measureName);
		
		LOGGER.debug("Pivoting " + measureName + " over " + identifierName);
	}

	@SuppressWarnings("unchecked")
	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata value = getThisMetadata(session);

		if (!(value instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(value, DataSetMetadata.class);

		DataSetMetadata dataset = (DataSetMetadata) value;

		DataStructureComponent<Identifier, ?, ?> tempIdentifier = dataset.getComponent(identifierName, Identifier.class).get();
		measure = dataset.getComponent(measureName, Measure.class).get();

		if (tempIdentifier == null)
			throw new VTLMissingComponentsException(identifierName, dataset.getComponents(Identifier.class));
		if (measure == null)
			throw new VTLMissingComponentsException(measureName, dataset.getComponents(Measure.class));
		if (!(tempIdentifier.getDomain() instanceof StringEnumeratedDomainSubset))
			throw new VTLException("pivot: " + tempIdentifier.getName() + " is of type " + tempIdentifier.getDomain() + " but should be of a StringEnumeratedDomainSubset.");

		// safe
		identifier = (DataStructureComponent<Identifier, StringEnumeratedDomainSubset, StringDomain>) tempIdentifier;
		
		return Utils.getStream(((StringEnumeratedDomainSubset) identifier.getDomain()).getCodeItems())
				.map(i -> DataStructureComponentImpl.of(i.get(), Measure.class, measure.getDomain()))
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
		
		return new LightFDataSet<>(structure, ds -> Utils.getStream(ds.stream()
			.collect(groupingByConcurrent(dp -> dp.getValues(ids, Identifier.class)))
			.entrySet())
			.map(group -> new DataPointBuilder(group.getKey()).addAll(Utils.getStream(group.getValue())
					.collect(toConcurrentMap(
							dp -> DataStructureComponentImpl.of(sanitize(dp.get(identifier)), Measure.class, measure.getDomain()), 
							dp -> dp.get(measure)
					))).build(structure)
			), dataset);
	}

	@Override
	public String toString()
	{
		return "[pivot " + identifierName + ", " + measureName + "]";
	}
}
