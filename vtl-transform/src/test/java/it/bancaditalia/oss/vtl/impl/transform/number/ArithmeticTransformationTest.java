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
package it.bancaditalia.oss.vtl.impl.transform.number;

import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleDataSets.SAMPLE1;
import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleDataSets.SAMPLE2;
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

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ArithmeticTransformationTest
{
	private TransformationScheme session;
	private VarIDOperand left, right;
	
	public static Stream<Arguments> twoDatasetTest()
	{
		return Stream.of(
				Arguments.of(SUM, new Long[] { 12L, null, 16L, null, 20L, 22L }, new Double[] { 11.1 + 1.1, null, 13.3 + 3.3, null, 15.5 + 5.5, NaN}),
				Arguments.of(DIFF, new Long[] { 10L, null, 10L, null, 10L, 10L }, new Double[] { 11.1 - 1.1, null, 13.3 - 3.3, null, 15.5 - 5.5, NaN}), 
				Arguments.of(MULT, new Long[] { 11L, null, 39L, null, 75L, 96L }, new Double[] { 11.1 * 1.1, null, 13.3 * 3.3, null, 15.5 * 5.5, NaN}), 
				Arguments.of(DIV, new Double[] { 11.0, null, 13.0 / 3, null, 3.0, 16.0 / 6 }, new Double[] { 11.1 / 1.1, null, 13.3 / 3.3, null, 15.5 / 5.5, NaN}),
				Arguments.of(MOD, new Long[] { 0L, null, 1L, null, 0L, 4L }, new Double[] { 11.1 % 1.1, null, 13.3 % 3.3, null, 15.5 % 5.5, NaN})
			);
	}
	
	@BeforeEach
	public void before()
	{
		left = new VarIDOperand(VTLAliasImpl.of("left"));
		right = new VarIDOperand(VTLAliasImpl.of("right"));
		Map<VTLAlias, DataSet> map = new HashMap<>();
		map.put(VTLAliasImpl.of("left"), SAMPLE2);
		map.put(VTLAliasImpl.of("right"), SAMPLE1);
		session = TestUtils.mockSession(map);
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	public void twoDatasetTest(ArithmeticOperator operator, Number[] integerResults, Double[] numberResults)
	{
		ArithmeticTransformation arTransformation = new ArithmeticTransformation(operator, left, right);
		
		DataSetStructure metadata = (DataSetStructure) arTransformation.getMetadata(session);
		assertTrue(metadata.contains(VTLAliasImpl.of("integer_1")));
		assertTrue(metadata.contains(VTLAliasImpl.of("number_1")));
		
		DataSet computedResult = (DataSet) arTransformation.eval(session);
		DataSetComponent<?, ?, ?> integerMeasure = metadata.getComponent(VTLAliasImpl.of("integer_1")).get();
		DataSetComponent<?, ?, ?> numberMeasure = metadata.getComponent(VTLAliasImpl.of("number_1")).get();
		
		assertEquals(integerResults.length, computedResult.size());
		assertEquals(metadata, computedResult.getMetadata());
		
		List<Number> longResults = Arrays.asList(integerResults);
		List<Number> longComputedResults = computedResult.stream()
				.map(dp -> dp.get(integerMeasure).get())
				.map(Number.class::cast)
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
