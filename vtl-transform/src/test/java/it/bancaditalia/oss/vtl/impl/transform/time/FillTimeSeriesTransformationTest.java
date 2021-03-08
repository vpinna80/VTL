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

import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE7;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE8;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE9;
import static it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode.SINGLE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

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
		Map<String, DataSet> map = new HashMap<>();
		map.put("operand", sample);
		session = TestUtils.mockSession(map);

		FillTimeSeriesTransformation ftsTransformation = new FillTimeSeriesTransformation(new VarIDOperand("operand"), mode);
		
		DataSet computedResult = (DataSet) ftsTransformation.eval(session);
		assertEquals(expectedSize, computedResult.size(), "Dataset size");
		
		DataStructureComponent<Identifier, DateDomainSubset, DateDomain> time_id = computedResult.getComponent("DATE_1", Identifier.class, DATEDS).get();
		DataStructureComponent<Identifier, StringDomainSubset, StringDomain> string_id = computedResult.getComponent("STRING_1", Identifier.class, STRINGDS).get();
		
		Collection<List<DataPoint>> splitResult = computedResult.stream().sequential()
				.sorted((dp1, dp2) -> (dp1.get(time_id)).compareTo(dp2.get(time_id)))
				.collect(Collectors.groupingBy(dp -> dp.get(string_id).get().toString()))
				.values();
		
		
		for (List<DataPoint> series: splitResult)
		{
			DateValue prev = null;
			for (DataPoint dp: series)
			{
				DateValue curr = (DateValue) dp.get(time_id);
				if (prev != null)
					assertTrue(prev.compareTo(curr) == 0, "Found hole: " + prev + " -- " + curr);
				else
					prev = curr;
				
				prev = prev.increment(1);
			}
		}
	}
}
