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
package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class TimeShiftTransformation extends TimeSeriesTransformation
{
	private static final long serialVersionUID = 1L;
	private final long amount;

	public TimeShiftTransformation(Transformation operand, ScalarValue<?, ?, ?, ?> amount)
	{
		super(operand);
		
		this.amount = ((IntegerValue<?, ?>) amount).get();
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		DataStructureComponent<Identifier, ?, ?> timeID = dataset.getMetadata().getComponents(Identifier.class, TIMEDS).iterator().next();
		DataSetMetadata structure = dataset.getMetadata();
		
		String lineageString = "timeshift " + amount;
		return new FunctionDataSet<>(structure, ds -> ds.stream()
				.map(dp -> new DataPointBuilder(dp)
					.delete(timeID)
					.add(timeID, ((TimeValue<?, ?, ?, ?>) dp.get(timeID)).increment(amount))
					.build(LineageNode.of(lineageString, dp.getLineage()), structure)
				), dataset);
	}

	@Override
	protected DataSetMetadata checkIsTimeSeriesDataSet(DataSetMetadata metadata, TransformationScheme scheme)
	{
		return metadata;
	}
	
	@Override
	public String toString()
	{
		return "timeshift(" + operand + ", " + amount + ")";
	}
}
