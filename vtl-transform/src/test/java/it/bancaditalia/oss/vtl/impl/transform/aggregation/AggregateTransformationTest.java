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
package it.bancaditalia.oss.vtl.impl.transform.aggregation;

import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE16;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE17;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE5;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE6;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.AVG;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.COUNT;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.MAX;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.MEDIAN;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.MIN;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.SUM;
import static java.lang.Double.NaN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class AggregateTransformationTest
{
	public static Stream<Arguments> test()
	{
		return Stream.of(
				Arguments.of(SUM, SAMPLE5,  69L),
				Arguments.of(SUM, SAMPLE6,  141L),
				Arguments.of(SUM, SAMPLE16, NaN),
				Arguments.of(SUM, SAMPLE17, 70.9),
				Arguments.of(AVG, SAMPLE5,  13L),
				Arguments.of(AVG, SAMPLE6,  23L),
				Arguments.of(AVG, SAMPLE16, NaN),
				Arguments.of(AVG, SAMPLE17, 14.18),
				Arguments.of(MEDIAN, SAMPLE5,  14L),
				Arguments.of(MEDIAN, SAMPLE6,  24L),
				Arguments.of(MEDIAN, SAMPLE16, 4.4),
				Arguments.of(MEDIAN, SAMPLE17, 14.95),
				Arguments.of(MIN, SAMPLE5,  11L),
				Arguments.of(MIN, SAMPLE6,  21L),
				Arguments.of(MIN, SAMPLE16, 1.1),
				Arguments.of(MIN, SAMPLE17, 11.1),
				Arguments.of(MAX, SAMPLE5,  16L),
				Arguments.of(MAX, SAMPLE6,  26L),
				Arguments.of(MAX, SAMPLE16, NaN),
				Arguments.of(MAX, SAMPLE17, 16.6),
				Arguments.of(COUNT, SAMPLE5,  6L),
				Arguments.of(COUNT, SAMPLE6,  6L),
				Arguments.of(COUNT, SAMPLE16, 6L),
				Arguments.of(COUNT, SAMPLE17, 6L)
			);
	}
	
	@ParameterizedTest(name = "{0} {1}")
	@MethodSource
	public void test(AggregateOperator operator, DataSet sample, Number result)
	{
		VarIDOperand operand = new VarIDOperand("operand");
		Map<String, DataSet> map = new HashMap<>();
		map.put("operand", sample);
		TransformationScheme session = TestUtils.mockSession(map);

		AggregateTransformation at = new AggregateTransformation(operator, operand, null, null);
		final VTLValueMetadata metadata = at.getMetadata(session);
		assertTrue(metadata instanceof DataSetMetadata, "Result structure is dataset: " + metadata);
		assertEquals(1, ((DataSetMetadata) metadata).size(), "Only one measure in " + metadata);

		final VTLValue eval = at.eval(session);
		assertTrue(eval instanceof DataSet, eval.getClass().getSimpleName() + " instanceof DataSet");
		DataSet dataset = (DataSet) eval;
		assertEquals(1, dataset.size(), "Only one datapoint in result");
		
		DataPoint dp = dataset.stream().findAny().get();
		Number value = ((Number) dp.values().iterator().next().get());
		assertEquals(result.getClass(), value.getClass(), "Integer preserved");
		if (value instanceof Double)
			assertEquals(result.doubleValue(), value.doubleValue(), 0.00001, "Result of " + operator);
		else
			assertEquals(result.longValue(), value.longValue(), "Result of " + operator);
	}
}
