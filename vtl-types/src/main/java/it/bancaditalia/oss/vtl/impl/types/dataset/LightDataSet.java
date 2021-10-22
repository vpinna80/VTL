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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import java.util.function.Supplier;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;

public class LightDataSet extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;
	private final Supplier<? extends Stream<DataPoint>> supplier;
	private final boolean cacheable;

	public LightDataSet(DataSetMetadata dataStructure, Supplier<? extends Stream<DataPoint>> datapoints, boolean cacheable)
	{
		super(dataStructure);
		
		this.supplier = datapoints;
		this.cacheable = cacheable;
	}
	
	public LightDataSet(DataSetMetadata dataStructure, Supplier<? extends Stream<DataPoint>> datapoints)
	{
		this(dataStructure, datapoints, true);
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return supplier.get();
	}
	
	@Override
	public boolean isCacheable()
	{
		return cacheable;
	}
}