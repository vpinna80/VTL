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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE11;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE12;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE4;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class FilterClauseTransformationTest
{
	private TransformationScheme session;
	private VarIDOperand condition;
	
	public static Stream<Arguments> test()
	{
		/*	
			{ true, true, null, false, false, true },
			{ true, false, false, false, true, null },
			{ true, false, true, false, true, false },
			{ true, null, null, false, true, null }
		*/
		return Stream.of(
				Arguments.of("1 of 4", SAMPLE3, new String[]{"A", "B", "F"}),
				Arguments.of("2 of 4", SAMPLE4, new String[]{"A", "E"}),
				Arguments.of("3 of 4", SAMPLE11, new String[]{"A", "C", "E"}),
				Arguments.of("4 of 4", SAMPLE12, new String[]{"A", "E"})
			);
	}

	@BeforeEach
	public void before()
	{
		condition = new VarIDOperand("BOOLEAN_1");
		Map<String, DataSet> map = new HashMap<>();
		session = TestUtils.mockSession(map);
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	public void test(String name, DataSet sample, String[] expectedResult)
	{
		session = new ThisScope(sample, session);
		FilterClauseTransformation fct = new FilterClauseTransformation(condition);
		
		DataSetMetadata metadata = (DataSetMetadata) fct.getMetadata(session);
		assertTrue(metadata.contains("BOOLEAN_1"));
		
		DataSet computedResult = (DataSet) fct.eval(session);
		
		assertEquals(expectedResult.length, computedResult.size());
		assertEquals(metadata, computedResult.getMetadata());

		DataStructureComponent<Identifier, StringDomainSubset, StringDomain> id = metadata.getComponent("STRING_1", Identifier.class, STRINGDS).get();
		
		String[] arrayResult = computedResult.stream()
			.map(dp -> dp.get(id).get())
			.sorted()
			.collect(toList())
			.toArray(new String[0]);
		
		assertArrayEquals(expectedResult, arrayResult, "Filtered values");
	}
}
