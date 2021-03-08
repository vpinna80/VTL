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
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_BOOLEAN_2;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_BOOLEAN_3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_INTEGER_1;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_INTEGER_2;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_INTEGER_3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_NUMBER_1;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleVariables.MEASURE_NUMBER_2;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.Utils;

public enum SampleDataSets implements DataSet
{
	SAMPLE3(new SampleVariables[] { IDENT_STRING_1, MEASURE_BOOLEAN_2 }),
	SAMPLE1(new SampleVariables[] { IDENT_STRING_1, IDENT_BOOLEAN_1, MEASURE_NUMBER_1, MEASURE_INTEGER_1}),
	SAMPLE2(new SampleVariables[] { IDENT_STRING_1, MEASURE_NUMBER_2, MEASURE_INTEGER_2 }),
	SAMPLE4(new SampleVariables[] { IDENT_STRING_1, MEASURE_BOOLEAN_3 }),
	SAMPLE5(new SampleVariables[] { IDENT_STRING_1, MEASURE_INTEGER_2 }),
	SAMPLE6(new SampleVariables[] { IDENT_STRING_1, MEASURE_INTEGER_3 }),
	SAMPLE7(new SampleVariables[] { IDENT_DATE_1, IDENT_STRING_5, MEASURE_INTEGER_2 }),
	SAMPLE8(new SampleVariables[] { IDENT_DATE_2, IDENT_STRING_6, MEASURE_INTEGER_2 }),
	SAMPLE9(new SampleVariables[] { IDENT_DATE_3, IDENT_STRING_7, MEASURE_INTEGER_2 }),
	SAMPLE10(new SampleVariables[] { IDENT_STRING_3, MEASURE_INTEGER_3 });

	private final DataSet dataset;

	private SampleDataSets(SampleVariables components[])
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
		
		return new LightDataSet(structure, () -> Utils.getStream(VAR_SAMPLE_LEN)
				.mapToObj(dpIdx -> {
					Map<String, AtomicInteger> counts = new HashMap<>(); 
					for (SampleVariables variable: variables)
						counts.put(variable.getType(), new AtomicInteger(1));
					
					return Arrays.stream(variables)
						.map(toEntry(
							var -> var.getComponent(counts.get(var.getType()).getAndIncrement()),
							var -> SampleValues.getValues(var.getType(), var.getIndex()).get(dpIdx) 
						)).collect(toDataPoint(structure));
				}));
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

	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues)
	{
		return dataset.getMatching(keyValues);
	}

	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet rightDataset, BiPredicate<DataPoint, DataPoint> filter,
			BinaryOperator<DataPoint> mergeOp)
	{
		return dataset.filteredMappedJoin(metadata, rightDataset, filter, mergeOp);
	}

	public DataSet filter(Predicate<DataPoint> predicate)
	{
		return dataset.filter(predicate);
	}

	public DataSet mapKeepingKeys(DataSetMetadata metadata,
			Function<? super DataPoint, ? extends Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>>> operator)
	{
		return dataset.mapKeepingKeys(metadata, operator);
	}

	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter, Collector<DataPoint, A, TT> groupCollector,
			BiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, T> finisher)
	{
		return dataset.streamByKeys(keys, filter, groupCollector, finisher);
	}
}
