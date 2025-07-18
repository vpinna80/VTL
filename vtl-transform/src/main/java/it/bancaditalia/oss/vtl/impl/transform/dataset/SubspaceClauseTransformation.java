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
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class SubspaceClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private final Map<VTLAlias, ScalarValue<?, ?, ?, ?>> subspace;
	
	public SubspaceClauseTransformation(Map<VTLAlias, ScalarValue<?, ?, ?, ?>> subspace)
	{
		this.subspace = requireNonNull(subspace);
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet operand = (DataSet) getThisValue(scheme);
		Map<DataSetComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> subspaceKeyValues = subspace.entrySet().stream()
				.map(keepingValue(operand::getComponent))
				.map(keepingValue(Optional::get))
				.collect(toConcurrentMap(e -> e.getKey().asRole(Identifier.class), Entry::getValue));
		
		return operand.subspace(subspaceKeyValues, lineageEnricher(this));
	}
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata operand = getThisMetadata(scheme);
		
		if (!(operand.isDataSet()))
			throw new VTLInvalidParameterException(operand, DataSetStructure.class);
		
		DataSetStructure dataset = (DataSetStructure) operand;
		
		Set<VTLAlias> missing = subspace.keySet().stream()
				.filter(name -> !dataset.getComponent(name, Identifier.class).isPresent())
				.collect(toSet());
		
		if (missing.size() > 0)
			throw new VTLMissingComponentsException(dataset.getIDs(), missing.toArray(VTLAlias[]::new));

		Set<DataSetComponent<Identifier, ?, ?>> keyValues = dataset.matchIdComponents(subspace.keySet(), "sub");
		
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
