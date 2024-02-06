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
import static java.util.Collections.singletonMap;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class IsNullTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOOL_MEASURE = DataStructureComponentImpl.of(Measure.class, BOOLEANDS);

	public IsNullTransformation(Transformation operand)
	{
		super(operand);
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return BooleanValue.of(scalar instanceof NullValue);
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		DataSetMetadata structure = new DataStructureBuilder(dataset.getMetadata().getIDs())
				.addComponent(BOOL_MEASURE)
				.build();

		DataStructureComponent<Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();

		return dataset.mapKeepingKeys(structure, dp -> LineageNode.of(this, dp.getLineage()), dp -> singletonMap(BOOL_MEASURE, BooleanValue.of(dp.get(measure) instanceof NullValue)));
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);

		if (meta instanceof ScalarValueMetadata)
			return BOOLEAN;
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getMeasures();
			if (dataset.getMeasures().size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, measures);

			return new DataStructureBuilder(dataset.getIDs())
					.addComponent(BOOL_MEASURE)
					.build();
		}
	}
	
	@Override
	public String toString()
	{
		return "isnull(" + operand + ")";
	}
}
