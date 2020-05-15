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
package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import it.bancaditalia.oss.vtl.impl.transform.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointImpl.DataPointBuilder;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class TimeShiftTransformation extends TimeSeriesTransformation
{
	private static final long serialVersionUID = 1L;
	private final long amount;

	public TimeShiftTransformation(Transformation operand, ScalarValue<?, ?, ?> amount)
	{
		super(operand);
		
		this.amount = ((IntegerValue) amount).get();
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		DataStructureComponent<Identifier, TimeDomainSubset<TimeDomain>, TimeDomain> timeID = dataset.getComponents(Identifier.class, TIMEDS).iterator().next();
		VTLDataSetMetadata structure = dataset.getMetadata();
		
		return new LightFDataSet<>(structure, ds -> ds.stream()
				.map(dp -> new DataPointBuilder(dp)
					.delete(timeID)
					.add(timeID, ((TimeValue<?, ?, ?>) dp.get(timeID)).increment(amount))
					.build(structure)
				), dataset);
	}

	@Override
	protected VTLDataSetMetadata checkIsTimeSeriesDataSet(VTLDataSetMetadata metadata, TransformationScheme scheme)
	{
		return metadata;
	}
	
	@Override
	public String toString()
	{
		return "timeshift(" + operand + ", " + amount + ")";
	}
}
