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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class SubspaceClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private final Map<String, ScalarValue<?, ?, ?, ?>> subspace;
	
	public SubspaceClauseTransformation(Map<String, ScalarValue<?, ?, ?, ?>> subspace)
	{
		this.subspace = subspace.keySet().stream().collect(toMap(Variable::normalizeAlias, subspace::get));
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		String lineageString = subspace.keySet().stream()
				.collect(joining(", ", "sub ", ""));
		
		DataSet operand = (DataSet) getThisValue(scheme);
		Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> subspaceKeyValues = subspace.entrySet().stream()
				.map(keepingValue(operand::getComponent))
				.map(keepingValue(Optional::get))
				.collect(toConcurrentMap(e -> e.getKey().asRole(Identifier.class), Entry::getValue));
		
		return operand.subspace(subspaceKeyValues, dp -> LineageNode.of(lineageString, dp.getLineage()));
	}
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata operand = getThisMetadata(scheme);
		
		if (!(operand instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(operand, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) operand;
		
		Set<String> missing = subspace.keySet().stream()
				.filter(name -> !dataset.getComponent(name, Identifier.class).isPresent())
				.collect(toSet());
		
		if (missing.size() > 0)
			throw new VTLMissingComponentsException(missing, dataset.getIDs());

		Set<DataStructureComponent<Identifier, ?, ?>> keyValues = dataset.matchIdComponents(subspace.keySet(), "sub");
		
		return dataset.subspace(keyValues);
	}
	
	@Override
	public String toString()
	{
		return subspace.entrySet().stream()
				.map(e -> e.getKey() + "=" + e.getValue())
				.collect(joining(", ", "sub ", ""));
	}
}
