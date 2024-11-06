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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class NotTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public NotTransformation(Transformation operand)
	{
		super(operand);
	}

	@Override
	protected VTLValue evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return BooleanValue.not(BOOLEANDS.cast(scalar));
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		Set<DataStructureComponent<Measure, ?, ?>> components = dataset.getMetadata().getMeasures();
		
		return dataset.mapKeepingKeys(dataset.getMetadata(), dp -> LineageNode.of(this, dp.getLineage()), dp -> {
					Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(components, Measure.class));
					map.replaceAll((c, v) -> BooleanValue.not(BOOLEANDS.cast(v)));
					return map;
				});
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);
		
		if (meta instanceof ScalarValueMetadata)
		{
			ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) meta).getDomain();
			if (BOOLEANDS.isAssignableFrom(domain))
				return BOOLEAN;
			else
				throw new VTLIncompatibleTypesException("not", BOOLEANDS, domain);
		}
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;
			
			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getMeasures();
			if (measures.size() == 0)
				throw new VTLMissingComponentsException(VTLAliasImpl.of("a measure"), dataset);
			
			measures.stream().forEach(m -> {
				if (!BOOLEANDS.isAssignableFrom(m.getVariable().getDomain()))
					throw new VTLIncompatibleTypesException(toString(), BOOLEANDS, m.getVariable().getDomain());
			});
			
			return dataset;
		}
	}
	
	@Override
	public String toString()
	{
		return "not " + operand;
	}
}
