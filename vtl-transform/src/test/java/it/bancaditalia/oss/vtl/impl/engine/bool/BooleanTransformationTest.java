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
package it.bancaditalia.oss.vtl.impl.engine.bool;

import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleDataSets.SAMPLE3;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleDataSets.SAMPLE4;
import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation.BooleanBiOperator.AND;
import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation.BooleanBiOperator.OR;
import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation.BooleanBiOperator.XOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.engine.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation;
import it.bancaditalia.oss.vtl.impl.transform.bool.BooleanTransformation.BooleanBiOperator;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class BooleanTransformationTest
{
	private TransformationScheme session;
	private VarIDOperand left, right;
	
	public static Stream<Arguments> test()
	{
//		BOOLEAN_DATAPOINTS_2(new Boolean[] { true, true, null, false, false, true }), 
//		BOOLEAN_DATAPOINTS_3(new Boolean[] { true, false, false, false, true, null });

		return Stream.of(
				Arguments.of(AND, new Boolean[] { true, false, null, false, false, null }),
				Arguments.of(OR, new Boolean[] { true, true, null, false, true, null }), 
				Arguments.of(XOR, new Boolean[] { false, true, null, false, true, null }) 
			);
	}
	
	@BeforeEach
	public void before()
	{
		left = new VarIDOperand("left");
		right = new VarIDOperand("right");
		Map<String, DataSet> map = new HashMap<>();
		map.put("left", SAMPLE3);
		map.put("right", SAMPLE4);
		session = TestUtils.mockSession(map);
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	public void test(BooleanBiOperator operator, Boolean[] booleanResults)
	{
		BooleanTransformation arTransformation = new BooleanTransformation(operator, left, right);
		
		DataSetMetadata metadata = (DataSetMetadata) arTransformation.getMetadata(session);
		assertTrue(metadata.contains("BOOLEAN_1"));
		
		DataSet computedResult = (DataSet) arTransformation.eval(session);
		
		assertEquals(booleanResults.length, computedResult.size());
		assertEquals(metadata, computedResult.getMetadata());
		
		DataStructureComponent<?, ?, ?> id = metadata.getComponent("STRING_1").get();
		DataStructureComponent<?, ?, ?> measure = metadata.getComponent("BOOLEAN_1").get();
		
		computedResult.stream()
			.map(dp -> new SimpleEntry<>(dp.get(id).get().toString().charAt(0) - 'A', dp.get(measure).get()))
			.forEach(e -> assertEquals(booleanResults[e.getKey()], e.getValue(), "" + (char)(e.getKey() + 'A')));

	}
}
