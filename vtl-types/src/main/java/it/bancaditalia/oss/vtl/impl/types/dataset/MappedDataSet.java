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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;

public final class MappedDataSet extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;
	
	private final DataSet source;
	private final SerFunction<? super DataPoint,? extends Map<? extends DataStructureComponent<?,?,?>,? extends ScalarValue<?,?,?,?>>> operator;
	private final SerFunction<? super DataPoint, ? extends Lineage> lineageOperator;

	public MappedDataSet(DataSetMetadata metadata, DataSet source, SerFunction<? super DataPoint,? extends Lineage> lineageOperator, SerFunction<? super DataPoint,? extends Map<? extends DataStructureComponent<?,?,?>,? extends ScalarValue<?,?,?,?>>> operator)
	{
		super(metadata);
		this.source = source;
		this.lineageOperator = lineageOperator;
		this.operator = operator;
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return source.stream().map(this::mapper);
	}

	private DataPoint mapper(DataPoint dp)
	{
		return new DataPointBuilder(dp.getValues(Identifier.class), DONT_SYNC)
					.addAll(operator.apply(dp))
					.build(lineageOperator.apply(dp), dataStructure);
	}

	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys, 
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter,
			SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<? super TT, ? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		return source.streamByKeys(keys, filter, mapping(this::mapper, groupCollector), finisher);
	}
}