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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class KeepClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;
	private final String names[];
	private VTLDataSetMetadata metadata;
	
	public KeepClauseTransformation(List<String> names)
	{
		this.names = names.toArray(new String[names.size()]);
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		return ((DataSet) getThisValue(session)).mapKeepingKeys(metadata, dp -> {
				Map<DataStructureComponent<? extends NonIdentifier, ?, ?>, ScalarValue<?, ?, ?>> map = new HashMap<>(dp.getValues(NonIdentifier.class));
				map.keySet().retainAll(metadata.getComponents(NonIdentifier.class));
				return map;
			});
	}

	@Override
	public VTLDataSetMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata operand = getThisMetadata(session);
		
		if (!(operand instanceof VTLDataSetMetadata))
			throw new VTLInvalidParameterException(operand, VTLDataSetMetadata.class);
		
		VTLDataSetMetadata dsMeta = (VTLDataSetMetadata) operand;
		List<String> missing = Arrays.stream(names)
				.filter(n -> !dsMeta.getComponent(n).isPresent())
				.collect(toList());
		
		if (!missing.isEmpty())
			throw new VTLMissingComponentsException(missing, (DataStructure) operand);
			
		Set<DataStructureComponent<Identifier, ?, ?>> namedIDs = Arrays.stream(names)
				.map(dsMeta::getComponent)
				.map(o -> o.orElse(null))
				.filter(Objects::nonNull)
				.filter(c -> c.is(Identifier.class))
				.map(c -> c.as(Identifier.class))
				.collect(toSet());
		
		if (!namedIDs.isEmpty())
			throw new VTLInvariantIdentifiersException("keep", namedIDs);

		metadata = dsMeta.keep(names);
		return metadata;
	}

	@Override
	public String toString()
	{
		return Arrays.stream(names).collect(joining(", ", "[keep ", "]"));
	}
}
