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
package it.bancaditalia.oss.vtl.impl.transform.testutils;

import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleValues.VAR_SAMPLE_LEN;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_BOOLEAN_1;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_DATE_1;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_DATE_2;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_DATE_3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_STRING_1;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_STRING_3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_STRING_5;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_STRING_6;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.IDENT_STRING_7;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_BOOLEAN_1;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_BOOLEAN_2;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_BOOLEAN_3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_BOOLEAN_4;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_INTEGER_1;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_INTEGER_2;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_INTEGER_3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_NUMBER_1;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_NUMBER_2;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_STRING_3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_STRING_4;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_STRING_8;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public enum SampleDataSets implements DataSet
{
	SAMPLE1(IDENT_STRING_1, IDENT_BOOLEAN_1, MEASURE_NUMBER_1, MEASURE_INTEGER_1),
	SAMPLE2(IDENT_STRING_1, MEASURE_NUMBER_2, MEASURE_INTEGER_2),
	SAMPLE3(IDENT_STRING_1, MEASURE_BOOLEAN_2),
	SAMPLE4(IDENT_STRING_1, MEASURE_BOOLEAN_3),
	SAMPLE5(IDENT_STRING_1, MEASURE_INTEGER_2),
	SAMPLE6(IDENT_STRING_1, MEASURE_INTEGER_3),
	SAMPLE7(IDENT_DATE_1, IDENT_STRING_5, MEASURE_INTEGER_2),
	SAMPLE8(IDENT_DATE_2, IDENT_STRING_6, MEASURE_INTEGER_2),
	SAMPLE9(IDENT_DATE_3, IDENT_STRING_7, MEASURE_INTEGER_2),
	SAMPLE10(IDENT_STRING_3, MEASURE_INTEGER_3),
	SAMPLE11(IDENT_STRING_1, MEASURE_BOOLEAN_1),
	SAMPLE12(IDENT_STRING_1, MEASURE_BOOLEAN_4),
	SAMPLE13(IDENT_STRING_1, MEASURE_STRING_8),
	SAMPLE14(IDENT_STRING_1, MEASURE_STRING_3),
	SAMPLE15(IDENT_STRING_1, MEASURE_STRING_4),
	SAMPLE16(IDENT_STRING_1, MEASURE_NUMBER_1),
	SAMPLE17(IDENT_STRING_1, MEASURE_NUMBER_2);

	private final DataSet dataset;

	private SampleDataSets(SampleVariables... components)
	{
		dataset = createSample(components);
	}

	public static DataSetMetadata createStructure(SampleVariables... variables)
	{
		Map<String, AtomicInteger> counts = new HashMap<>(); 
		
		for (SampleVariables variable: variables)
			counts.put(variable.getType(), new AtomicInteger(1));
		
		return Arrays.stream(variables)
			.map(variable -> variable.getComponent(counts.get(variable.getType()).getAndIncrement()))
			.collect(DataStructureBuilder.toDataStructure());
	}
	
	private static DataSet createSample(SampleVariables variables[])
	{
		DataSetMetadata structure = createStructure(variables);
		
		return new StreamWrapperDataSet(structure, () -> Utils.getStream(VAR_SAMPLE_LEN)
				.mapToObj(dpIdx -> {
					Map<String, AtomicInteger> counts = new HashMap<>(); 
					for (SampleVariables variable: variables)
						counts.put(variable.getType(), new AtomicInteger(1));
					
					return Arrays.stream(variables)
						.map(toEntry(
							var -> var.getComponent(counts.get(var.getType()).getAndIncrement()),
							var -> SampleValues.getValues(var.getType(), var.getIndex()).get(dpIdx) 
						)).collect(toDataPoint(mock(LineageNode.class), structure));
				}));
	}

	@Override
	public DataSet subspace(Map<? extends DataStructureComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keyValues, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator)
	{
		DataSetMetadata newMetadata = new DataStructureBuilder(dataset.getMetadata()).removeComponents(keyValues.keySet()).build();
		
		return new AbstractDataSet(newMetadata)
		{
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return dataset.stream()
						.filter(dp -> dp.matches(keyValues))
						.map(dp -> new DataPointBuilder(dp).delete(keyValues.keySet()).build(LineageNode.of("SUB" + keyValues, dp.getLineage()), newMetadata));
			}
		};
}
	
	public static DataSet getCustomSample(String domain, int level)
	{
		return createSample(new SampleVariables[] { IDENT_STRING_1, SampleVariables.valueOf("MEASURE_" + domain + "_" + level) });
	}

	public Stream<DataPoint> stream()
	{
		return dataset.stream();
	}

	public DataSetMetadata getMetadata()
	{
		return dataset.getMetadata();
	}

	public DataSet membership(String component)
	{
		return dataset.membership(component);
	}

	public Optional<DataStructureComponent<?, ?, ?>> getComponent(String name)
	{
		return dataset.getComponent(name);
	}

	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return dataset.getMatching(keyValues);
	}

	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet rightDataset, SerBiPredicate<DataPoint, DataPoint> filter,
			SerBinaryOperator<DataPoint> mergeOp, boolean leftJoin)
	{
		return dataset.filteredMappedJoin(metadata, rightDataset, filter, mergeOp, false);
	}

	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate, SerUnaryOperator<Lineage> lineageOperator)
	{
		return dataset.filter(predicate, lineageOperator);
	}
	
	public DataSet mapKeepingKeys(DataSetMetadata metadata,
			SerFunction<? super DataPoint, ? extends Lineage> lineageOperator, SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		return dataset.mapKeepingKeys(metadata, x -> mock(Lineage.class), operator);
	}

	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter, SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		return dataset.streamByKeys(keys, filter, groupCollector, finisher);
	}

	@Override
	public DataSet aggr(DataSetMetadata structure, Set<DataStructureComponent<Identifier, ?, ?>> keys,
			SerCollector<DataPoint, ?, DataPoint> groupCollector,
			SerBiFunction<DataPoint, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> finisher)
	{
		return dataset.aggr(structure, keys, groupCollector, finisher);
	}

	@Override
	public <TT> DataSet analytic(SerFunction<DataPoint, Lineage> lineageOp, 
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components,
			WindowClause clause,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors,
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers)
	{
		return dataset.analytic(DataPoint::getLineage, components, clause, collectors, finishers);
	}
	
	@Override
	public DataSet union(SerFunction<DataPoint, Lineage> lineageOp, List<DataSet> others, boolean check)
	{
		return dataset.union(lineageOp, others);
	}
	
	@Override
	public DataSet setDiff(DataSet other)
	{
		return dataset.setDiff(other);
	}
	
	@Override
	public DataSet flatmapKeepingKeys(DataSetMetadata metadata, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Stream<? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>>> operator)
	{
		throw new UnsupportedOperationException();
	}
}
