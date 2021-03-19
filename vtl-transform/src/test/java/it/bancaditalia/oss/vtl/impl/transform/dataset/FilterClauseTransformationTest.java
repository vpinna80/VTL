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

import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation.BooleanBiOperator.AND;
import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation.BooleanBiOperator.OR;
import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation.BooleanBiOperator.XOR;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE3;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE4;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE11;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE12;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation.BooleanBiOperator;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class FilterClauseTransformationTest
{
	private TransformationScheme session;
	private VarIDOperand condition;
	
	public static Stream<Arguments> test()
	{
		return Stream.of(
				Arguments.of(SAMPLE3, 3),
				Arguments.of(SAMPLE4, 2),
				Arguments.of(SAMPLE11, 3),
				Arguments.of(SAMPLE12, 2)
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
	public void test(DataSet sample, int resultSize)
	{
		session = new ThisScope(sample, session);
		FilterClauseTransformation fct = new FilterClauseTransformation(condition);
		
		DataSetMetadata metadata = (DataSetMetadata) fct.getMetadata(session);
		assertTrue(metadata.contains("BOOLEAN_1"));
		
		DataSet computedResult = (DataSet) fct.eval(session);
		
		assertEquals(resultSize, computedResult.size());
		assertEquals(metadata, computedResult.getMetadata());
	}
}
