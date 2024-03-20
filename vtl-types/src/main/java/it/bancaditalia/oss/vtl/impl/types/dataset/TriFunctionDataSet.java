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

import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.util.SerTriFunction;

public class TriFunctionDataSet<A, B, C> extends StreamWrapperDataSet
{
	private static final long serialVersionUID = 1L;
	private final SerTriFunction<? super A, ? super B, ? super C, ? extends Stream<DataPoint>> function;
	private final A p1;
	private final B p2;
	private final C p3;

	public TriFunctionDataSet(DataSetMetadata dataStructure, SerTriFunction<? super A, ? super B, ? super C, Stream<DataPoint>> datapoints, A p1, B p2, C p3)
	{
		super(dataStructure, null);
		this.function = datapoints;
		this.p1 = p1;
		this.p2 = p2;
		this.p3 = p3;
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return function.apply(p1, p2, p3);
	}
}