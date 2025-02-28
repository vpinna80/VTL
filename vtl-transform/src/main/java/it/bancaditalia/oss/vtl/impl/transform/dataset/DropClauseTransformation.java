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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DropClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;

	private final VTLAlias[] names;
	
	public DropClauseTransformation(List<VTLAlias> names)
	{
		this.names = names.toArray(new VTLAlias[names.size()]);
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) getThisValue(scheme);
		Set<DataStructureComponent<NonIdentifier, ?, ?>> toDrop = Arrays.stream(names)
				.map(n -> dataset.getComponent(n))
				.map(Optional::get)
				.map(c -> c.asRole(NonIdentifier.class))
				.collect(toSet());
		
		return dataset.mapKeepingKeys((DataSetMetadata) getMetadata(scheme), lineage -> LineageNode.of(this, lineage), dp -> {
					Map<DataStructureComponent<? extends NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> newVals = new HashMap<>(dp.getValues(NonIdentifier.class));
					newVals.keySet().removeAll(toDrop);
					return newVals;
				});
	}
	
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata operand = getThisMetadata(scheme);
		
		if (!(operand.isDataSet()))
			throw new VTLInvalidParameterException(operand, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) operand;
		DataStructureBuilder builder = new DataStructureBuilder((DataSetMetadata) operand);

		for (VTLAlias name: names)
		{
			DataStructureComponent<?, ?, ?> c = dataset.getComponent(name).orElseThrow(() -> new VTLMissingComponentsException(name, dataset));
			if (c.is(Identifier.class))
				throw new VTLInvariantIdentifiersException("drop", singleton(c.asRole(Identifier.class)));
			builder.removeComponent(c);
		}
		
		return builder.build();
	}

	@Override
	public String toString()
	{
		return Arrays.stream(names).map(VTLAlias::toString).collect(joining(", ", "drop ", ""));
	}
}
