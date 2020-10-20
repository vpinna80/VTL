/**
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

import java.util.function.BiFunction;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;

public class LightF2DataSet<P, Q> extends LightDataSet
{
	private static final long serialVersionUID = 1L;
	private final BiFunction<? super P, ? super Q, ? extends Stream<DataPoint>> function;
	private final P p1;
	private final Q p2;

	public LightF2DataSet(DataSetMetadata dataStructure, BiFunction<? super P, ? super Q, Stream<DataPoint>> datapoints, P p1, Q p2)
	{
		super(dataStructure, null);
		this.function = datapoints;
		this.p1 = p1;
		this.p2 = p2;
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return function.apply(p1, p2);
	}
}