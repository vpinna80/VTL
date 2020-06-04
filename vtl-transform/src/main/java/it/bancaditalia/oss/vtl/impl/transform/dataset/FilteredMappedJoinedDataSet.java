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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class FilteredMappedJoinedDataSet extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;

	private final static Logger LOGGER = LoggerFactory.getLogger(FilteredMappedJoinedDataSet.class);

	private final BiPredicate<DataPoint, DataPoint>           predicate;
	private final BinaryOperator<DataPoint>                         mergeOp;
	private final Set<DataStructureComponent<Identifier, ?, ?>> indexKeys;
	private final DataSet                                                 left;
	private final DataSet                                                 right;

	FilteredMappedJoinedDataSet(VTLDataSetMetadata dataStructure, DataSet left, DataSet right, BiPredicate<DataPoint, DataPoint> predicate, BinaryOperator<DataPoint> mergeOp)
	{
		super(dataStructure);
		
		LOGGER.debug("Creating dataset by joining {} and {} into {}", left.getDataStructure(), right.getDataStructure(), dataStructure);

		Set<DataStructureComponent<Identifier, ?, ?>> indexingKeys = new HashSet<>(left.getComponents(Identifier.class));
		indexingKeys.retainAll(right.getComponents(Identifier.class));

		this.left = left;
		this.right = right;
		this.predicate = predicate;
		this.mergeOp = mergeOp;
		this.indexKeys = indexingKeys;
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return right.stream()
				.map(dpOther -> left.getMatching(dpOther.getValues(indexKeys, Identifier.class))
				.filter(dp -> predicate.test(dp, dpOther))
				.map(dp -> mergeOp.apply(dp, dpOther)))
				.reduce(Stream::concat)
				.orElse(Stream.empty());
	}

	@Override
	public Stream<DataPoint> getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues)
	{
		return left.getMatching(keyValues)
				.map(dpLeft -> right.getMatching(dpLeft.getValues(indexKeys, Identifier.class))
						.filter(dpRight -> predicate.test(dpLeft, dpRight))
						.map(dpRight -> mergeOp.apply(dpLeft, dpRight)))
				.reduce(Stream::concat)
				.orElse(Stream.empty());
	}

	@Override
	public <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter,
			BiFunction<? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, ? super Stream<DataPoint>, T> groupMapper)
	{
		return left.streamByKeys(keys, filter, (keyValues, group) -> {
			LOGGER.trace("Working on indexed dataset group {}", keyValues);
			return groupMapper.apply(keyValues, group
					.map(dpLeft -> right.getMatching(dpLeft.getValues(indexKeys, Identifier.class))
							.filter(dpRight -> predicate.test(dpLeft, dpRight))
							.map(dpRight -> mergeOp.apply(dpLeft, dpRight)))
					.reduce(Stream::concat)
					.orElse(Stream.empty()));
		});
	}
}