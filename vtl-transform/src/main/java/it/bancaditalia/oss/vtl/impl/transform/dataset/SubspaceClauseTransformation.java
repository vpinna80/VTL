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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toSet;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class SubspaceClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private final Map<String, ScalarValue<?, ?, ?>> subspace;
	private DataSetMetadata metadata;
	
	public SubspaceClauseTransformation(Map<String, ScalarValue<?, ?, ?>> subspace)
	{
		this.subspace = subspace;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet operand = (DataSet) getThisValue(session);
		Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> subspaceKeyValues = Utils.getStream(subspace.entrySet())
				.collect(toConcurrentMap(e -> operand.getComponent(e.getKey()).get().as(Identifier.class), Entry::getValue));
		
		return new LightDataSet(metadata, () -> operand.stream()
				.filter(dp -> subspaceKeyValues.equals(dp.getValues(subspaceKeyValues.keySet(), Identifier.class)))
				.map(dp -> new DataPointBuilder(dp).delete(subspaceKeyValues.keySet()).build(metadata)));
	}

	@Override
	public DataSetMetadata getMetadata(TransformationScheme session)
	{
		if (metadata != null)
			return metadata;
		
		VTLValueMetadata operand = getThisMetadata(session);
		
		if (!(operand instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(operand, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) operand;
		
		Set<String> missing = subspace.keySet().stream()
				.filter(name -> dataset.getComponent(name, Identifier.class) == null)
				.collect(Collectors.toSet());
		
		if (missing.size() > 0)
			throw new VTLMissingComponentsException(missing, dataset);

		// TODO: cast because compiler bug
		metadata = dataset.subspace(subspace.keySet().stream()
				.map(name -> dataset.getComponent(name, Identifier.class).get()).collect(toSet()));
		
		return metadata;
	}
	
	@Override
	public String toString()
	{
		return subspace.entrySet().stream()
				.map(e -> e.getKey() + "=" + e.getValue())
				.collect(joining(", ", "[sub ", "]"));
	}
}
