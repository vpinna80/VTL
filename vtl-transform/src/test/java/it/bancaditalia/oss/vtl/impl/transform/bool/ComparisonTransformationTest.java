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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static java.lang.Double.isNaN;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.operators.ComparisonOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ComparisonTransformationTest
{
	public static Stream<Arguments> test()
	{
		Boolean results[][] = { 
				// INTEGER
				{ false, null, false, null, false, false }, // EQ
				{ true,  null, true,  null, true,  true  }, // NE
				{ false, null, false, null, false, false }, // GT
				{ false, null, false, null, false, false }, // GE
				{ true,  null, true,  null, true,  true  }, // LT
				{ true,  null, true,  null, true,  true  }, // LE
				// NUMBER {@see Double#compareTo(Double anotherDouble)}
				{ 1.1 == 11.1, null, 3.3 == 13.3, null, 5.5 == 15.5, isNaN(16.6)             }, // EQ  
				{ 1.1 != 11.1, null, 3.3 != 13.3, null, 5.5 != 15.5, !isNaN(16.6)            }, // NE
				{ 1.1 >  11.1, null, 3.3 >  13.3, null, 5.5 >  15.5, true  /* NaN >  16.6 */ }, // GT
				{ 1.1 >= 11.1, null, 3.3 >= 13.3, null, 5.5 >= 15.5, true  /* NaN >= 16.6 */ }, // GE  
				{ 1.1 <  11.1, null, 3.3 <  13.3, null, 5.5 <  15.5, false /* NaN <  16.6 */ }, // LT  
				{ 1.1 <= 11.1, null, 3.3 <= 13.3, null, 5.5 <= 15.5, false /* NaN <= 16.6 */ }, // LE  
				// STRING
				{ 'H' == 'A', 'I' == 'C', 'J' == 'E', 'K' == 'G', 'L' == 'I', 'M' == 'K' }, // EQ
				{ 'H' != 'A', 'I' != 'C', 'J' != 'E', 'K' != 'G', 'L' != 'I', 'M' != 'K' }, // NE
				{ 'H' >  'A', 'I' >  'C', 'J' >  'E', 'K' >  'G', 'L' >  'I', 'M' >  'K' }, // GT
				{ 'H' >= 'A', 'I' >= 'C', 'J' >= 'E', 'K' >= 'G', 'L' >= 'I', 'M' >= 'K' }, // GE
				{ 'H' <  'A', 'I' <  'C', 'J' <  'E', 'K' <  'G', 'L' <  'I', 'M' <  'K' }, // LT
				{ 'H' <= 'A', 'I' <= 'C', 'J' <= 'E', 'K' <= 'G', 'L' <= 'I', 'M' <= 'K' }  // LE
			};
		
		Queue<Boolean[]> queue = new LinkedList<>(Arrays.asList(results));
		
		return Stream.of("INTEGER", "NUMBER", "STRING")
				.flatMap(domain -> Arrays.stream(ComparisonOperator.values())
						.map(op -> Arguments.of(op, domain, queue.remove())));
	}
	
	@ParameterizedTest(name = "{0} - {1}")
	@MethodSource
	public synchronized void test(ComparisonOperator operator, String measureDomain, Boolean result[])
	{
		VarIDOperand left = new VarIDOperand("left"), right = new VarIDOperand("right");
		Map<String, DataSet> map = new HashMap<>();
		map.put("left", SampleDataSets.getCustomSample(measureDomain, "STRING".equals(measureDomain) ? 2 : 1));
		map.put("right", SampleDataSets.getCustomSample(measureDomain, "STRING".equals(measureDomain) ? 3 : 2));
		TransformationScheme session = TestUtils.mockSession(map);

		ComparisonTransformation coTransformation = new ComparisonTransformation(operator, left, right);
		
		DataSetMetadata metadata = (DataSetMetadata) coTransformation.getMetadata(session);
		assertTrue(metadata.contains("bool_var"));
		
		DataSet computedResult = (DataSet) coTransformation.eval(session);
		
		DataStructureComponent<?, ?, ?> id = metadata.getComponent("STRING_1").get();		
		DataStructureComponent<?, ?, ?> bool_var = metadata.getComponent("bool_var").get();		
		
		DataSet leftD = (DataSet) left.eval(session), 
				rightD = (DataSet) right.eval(session);
		
		for (DataPoint dp: computedResult)
		{
			final int index = dp.get(id).get().toString().charAt(0) - 'A';
			final ScalarValue<?, ?, ?> idValue = dp.get(id);

			final String message = Stream.of(leftD, rightD)
					.map(DataSet::stream)
					.map(st -> st.filter(dp2 -> dp2.get(id).equals(idValue)))
					.map(Stream::findFirst)
					.map(Optional::get)
					.map(dp2 -> dp2.getValues(Measure.class).values().iterator().next().toString())
					.collect(joining(operator.toString(), dp.get(id).get().toString(), ")"));

			assertEquals(result[index], dp.get(bool_var).get(), message);
		}
	}
}
