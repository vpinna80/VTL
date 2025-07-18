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

import java.util.function.Function;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.util.SerFunction;

public class FunctionDataSet<P> extends StreamWrapperDataSet
{
	private final Function<? super P, ? extends Stream<DataPoint>> function;
	private final P param;

	public FunctionDataSet(DataSetStructure dataStructure, SerFunction<? super P, ? extends Stream<DataPoint>> datapoints, P param, boolean cacheable)
	{
		super(dataStructure, null, cacheable);
		this.function = datapoints;
		this.param = param;
	}

	public FunctionDataSet(DataSetStructure dataStructure, SerFunction<? super P, ? extends Stream<DataPoint>> datapoints, P param)
	{
		this(dataStructure, datapoints, param, true);
	}
	
	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return function.apply(param);
	}
	
	@Override
	public boolean isCacheable()
	{
		return super.isCacheable();
	}
}