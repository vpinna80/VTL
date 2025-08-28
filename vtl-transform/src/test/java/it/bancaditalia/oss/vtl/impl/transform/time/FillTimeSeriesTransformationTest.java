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

import static it.bancaditalia.oss.vtl.impl.environment.samples.SampleDataSets.SAMPLE7;
import static it.bancaditalia.oss.vtl.impl.environment.samples.SampleDataSets.SAMPLE8;
import static it.bancaditalia.oss.vtl.impl.environment.samples.SampleDataSets.SAMPLE9;
import static it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode.SINGLE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static java.util.stream.Collectors.groupingBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class FillTimeSeriesTransformationTest
{
	private TransformationScheme session;
	
	public static Stream<Arguments> test()
	{
		// 7: 2020-01-01 2020-01-02 2020-01-03 2020-01-04 2020-01-05 2020-01-06
		// 8: 2020-01-01            2020-01-03            2020-01-05            2020-01-07            2020-01-09            2020-01-11 
		// 9:                       2020-01-03 2020-01-04 2020-01-05            2020-01-07 2020-01-08            2020-01-10

		return Stream.of(
				Arguments.of("SAMPLE7",                 TestUtils.concat(SAMPLE7),                   SINGLE,  6),
				Arguments.of("SAMPLE8",                 TestUtils.concat(SAMPLE8),                   SINGLE, 11),
				Arguments.of("SAMPLE9",                 TestUtils.concat(SAMPLE9),                   SINGLE,  8),
				Arguments.of("SAMPLE7 SAMPLE8",         TestUtils.concat(SAMPLE7, SAMPLE8),          SINGLE, 17),
				Arguments.of("SAMPLE7 SAMPLE8",         TestUtils.concat(SAMPLE7, SAMPLE8),          ALL,    22),
				Arguments.of("SAMPLE8 SAMPLE9",         TestUtils.concat(SAMPLE8, SAMPLE9),          SINGLE, 19),
				Arguments.of("SAMPLE8 SAMPLE9",         TestUtils.concat(SAMPLE8, SAMPLE9),          ALL,    22),
				Arguments.of("SAMPLE7 SAMPLE8 SAMPLE9", TestUtils.concat(SAMPLE7, SAMPLE8, SAMPLE9), ALL,    33)
			);
	}
	
	@ParameterizedTest(name = "{0} {2}")
	@MethodSource
	public void test(String name, DataSet sample, FillMode mode, int expectedSize)
	{
		Map<VTLAlias, DataSet> map = new HashMap<>();
		map.put(VTLAliasImpl.of("operand"), sample);
		session = TestUtils.mockSession(map);

		FillTimeSeriesTransformation ftsTransformation = new FillTimeSeriesTransformation(new VarIDOperand(VTLAliasImpl.of("operand")), mode);
		DataSet computedResult = (DataSet) ftsTransformation.eval(session);
		DataSetComponent<Identifier, ?, ?> time_id = computedResult.getMetadata().getComponent(VTLAliasImpl.of("date_1"), Identifier.class, DATEDS).get();
		DataSetComponent<Identifier, ?, ?> string_id = computedResult.getMetadata().getComponent(VTLAliasImpl.of("string_1"), Identifier.class, STRINGDS).get();

		assertEquals(expectedSize, computedResult.size(), "Number of datapoints");

		if (mode == SINGLE)
		{
			Collection<List<DataPoint>> splitResult = computedResult.stream().sequential()
				.sorted((dp1, dp2) -> (dp1.get(time_id)).compareTo(dp2.get(time_id)))
				.collect(groupingBy(dp -> dp.get(string_id).get().toString()))
				.values();
			
			for (List<DataPoint> series: splitResult)
			{
				DateValue<?> prev = null;
				for (DataPoint dp: series)
				{
					DateValue<?> curr = (DateValue<?>) dp.get(time_id);
					if (prev != null)
						assertTrue(prev.compareTo(curr) == 0, "Found hole: " + prev + " -- " + curr);
					else
						prev = curr;
					
					prev = prev.add(1);
				}
			}
		}
		else
		{
			ConcurrentMap<ScalarValue<?, ?, ?, ?>, Long> results = computedResult.stream()
				.collect(groupingByConcurrent(dp -> dp.get(time_id), counting()));
			
			long nSeries = results.values().stream().mapToLong(Long::longValue).max().getAsLong();
			
			ScalarValue<?, ?, ?, ?> min = Utils.getStream(results.keySet())
					.filter(not(NullValue.class::isInstance))
					.collect(collectingAndThen(minBy(DateValue.class, ScalarValue::compareTo), Optional::get));
			ScalarValue<?, ?, ?, ?> max = Utils.getStream(results.keySet())
					.filter(not(NullValue.class::isInstance))
					.collect(collectingAndThen(maxBy(DateValue.class, ScalarValue::compareTo), Optional::get));
			
			DateValue<?> curr = (DateValue<?>) min;
			while (curr.compareTo(max) < 0)
			{
				assertNotNull(results.get(curr), "Found hole: " + curr);
				assertEquals(nSeries, results.get(curr), "Found hole: " + curr);
				curr = curr.add(1);
			}
		}
	}
}
