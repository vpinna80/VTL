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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toArray;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class KeepClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private final String names[];
	
	public KeepClauseTransformation(List<String> names)
	{
		this.names = names.stream().map(Variable::normalizeAlias).collect(toArray(new String[names.size()]));
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);
		
		return ((DataSet) getThisValue(scheme)).mapKeepingKeys(metadata, dp -> LineageNode.of(this, dp.getLineage()), dp -> {
				var map = new HashMap<>(dp.getValues(NonIdentifier.class));
				map.keySet().retainAll(metadata.getComponents(NonIdentifier.class));
				return map;
			});
	}
	
	public DataSetMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata operand = getThisMetadata(scheme);
		
		if (!(operand instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(operand, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) operand;
		
		Set<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> namedComps = Arrays.stream(names)
				.map(toEntryWithValue(dataset::getComponent))
				.map(e -> e.getValue().orElseThrow(() -> new VTLMissingComponentsException(e.getKey(), dataset)))
				.peek(c -> { if (c.is(Identifier.class)) throw new VTLInvariantIdentifiersException("keep", singleton(c.asRole(Identifier.class))); })
				.map(c -> c.asRole(NonIdentifier.class))
				.collect(toSet());

		return new DataStructureBuilder(dataset.getComponents(Identifier.class))
				.addComponents(namedComps)
				.build();
	}

	@Override
	public String toString()
	{
		return Arrays.stream(names).collect(joining(", ", "keep ", ""));
	}
}
