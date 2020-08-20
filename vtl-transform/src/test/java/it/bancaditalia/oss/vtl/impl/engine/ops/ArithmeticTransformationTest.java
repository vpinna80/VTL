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
package it.bancaditalia.oss.vtl.impl.engine.ops;

import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleDataSets.SAMPLE1;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleDataSets.SAMPLE2;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.DIFF;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.DIV;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.MOD;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.MULT;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.SUM;
import static java.lang.Double.NaN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.engine.testutils.MockSession;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.number.ArithmeticTransformation;
import it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class ArithmeticTransformationTest
{
	private MockSession session;
	private VarIDOperand left, right;
	
	public static Stream<Arguments> twoDatasetTest()
	{
//		INTEGER_DATAPOINTS_1(new Long[] { 1L, 2L, 3L, 4L, 5L, 6L }), 
//		INTEGER_DATAPOINTS_2(new Long[] { 11L, 12L, 13L, 14L, 15L, 16L }),
//
//		NUMBER_DATAPOINTS_1(new Double[] { 1.1, 2.2, 3.3, 4.4, 5.5, 6.6 }), 
//		NUMBER_DATAPOINTS_2(new Double[] { 11.1, 12.2, 13.3, 14.4, 15.5, 16.6 }), 

		return Stream.of(
				Arguments.of(SUM, new Long[] { 12L, null, 16L, null, 20L, 22L }, new Double[] { 11.1 + 1.1, null, 13.3 + 3.3, null, 15.5 + 5.5, NaN}),
				Arguments.of(DIFF, new Long[] { 10L, null, 10L, null, 10L, 10L }, new Double[] { 11.1 - 1.1, null, 13.3 - 3.3, null, 15.5 - 5.5, NaN}), 
				Arguments.of(MULT, new Long[] { 11L, null, 39L, null, 75L, 96L }, new Double[] { 11.1 * 1.1, null, 13.3 * 3.3, null, 15.5 * 5.5, NaN}), 
				Arguments.of(DIV, new Long[] { 11L, null, 4L, null, 3L, 2L }, new Double[] { 11.1 / 1.1, null, 13.3 / 3.3, null, 15.5 / 5.5, NaN}),
				Arguments.of(MOD, new Long[] { 0L, null, 1L, null, 0L, 4L }, new Double[] { 11.1 % 1.1, null, 13.3 % 3.3, null, 15.5 % 5.5, NaN})
			);
	}
	
	@BeforeEach
	public void before()
	{
		left = new VarIDOperand("left");
		right = new VarIDOperand("right");
		Map<String, DataSet> map = new HashMap<>();
		map.put("left", SAMPLE2);
		map.put("right", SAMPLE1);
		session = new MockSession(map);
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	public void twoDatasetTest(ArithmeticOperator operator, Long[] integerResults, Double[] numberResults)
	{
		ArithmeticTransformation arTransformation = new ArithmeticTransformation(operator, left, right);
		
		VTLDataSetMetadata metadata = (VTLDataSetMetadata) arTransformation.getMetadata(session);
		assertTrue(metadata.contains("INTEGER_1"));
		assertTrue(metadata.contains("NUMBER_1"));
		
		DataSet computedResult = (DataSet) arTransformation.eval(session);
		DataStructureComponent<?, ?, ?> integerMeasure = metadata.getComponent("INTEGER_1").get();
		DataStructureComponent<?, ?, ?> numberMeasure = metadata.getComponent("NUMBER_1").get();
		
		assertEquals(integerResults.length, computedResult.size());
		assertEquals(metadata, computedResult.getDataStructure());
		
		List<Long> longResults = Arrays.asList(integerResults);
		List<Long> longComputedResults = computedResult.stream()
				.map(dp -> dp.get(integerMeasure).get())
				.map(Long.class::cast)
				.collect(Collectors.toList());
		longComputedResults.forEach(l -> assertTrue(longResults.contains(l), "Int value " + l + " should be one of " + longResults));

		List<Double> doubleResults = Arrays.asList(numberResults);
		List<Double> doubleComputedResults = computedResult.stream()
				.map(dp -> dp.get(numberMeasure).get())
				.map(Double.class::cast)
				.collect(Collectors.toList());
		doubleComputedResults.forEach(l -> assertTrue(doubleResults.contains(l), "Double value " + l + " should be one of " + doubleResults));
	}
}
