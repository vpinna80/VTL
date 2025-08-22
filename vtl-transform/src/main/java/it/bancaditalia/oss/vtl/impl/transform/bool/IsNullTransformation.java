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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.Collections.singletonMap;

import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class IsNullTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public IsNullTransformation(Transformation operand)
	{
		super(operand);
	}

	@Override
	protected VTLValue evalOnScalar(TransformationScheme scheme, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return BooleanValue.of(scalar.isNull());
	}

	@Override
	protected VTLValue evalOnDataset(TransformationScheme scheme, DataSet dataset, VTLValueMetadata metadata)
	{
		DataSetStructure structure = new DataSetStructureBuilder(dataset.getMetadata().getIDs())
				.addComponent(BOOL_VAR)
				.build();

		DataSetComponent<Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();

		return dataset.mapKeepingKeys(structure, lineageEnricher(this), dp -> singletonMap(BOOL_VAR, BooleanValue.of(dp.get(measure).isNull())));
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);

		if (!meta.isDataSet())
			return BOOLEAN;
		else
		{
			DataSetStructure dataset = (DataSetStructure) meta;

			Set<? extends DataSetComponent<? extends Measure, ?, ?>> measures = dataset.getMeasures();
			if (dataset.getMeasures().size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, measures);

			return new DataSetStructureBuilder(dataset.getIDs())
					.addComponent(BOOL_VAR)
					.build();
		}
	}
	
	@Override
	public String toString()
	{
		return "isnull(" + operand + ")";
	}
}
