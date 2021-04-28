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
		// "Filled String", "    Leading spaces", "Trailing spaces    ", "    Leading and trailing     ", "\"Quoted\" 'String'", "\t\b \n\r\f"

		return Stream.of(
				Arguments.of(SUM, SAMPLE5,  69.),
				Arguments.of(SUM, SAMPLE6,  141.),
				Arguments.of(SUM, SAMPLE16, NaN),
				Arguments.of(SUM, SAMPLE17, 70.9)
			);
	}
	
	@ParameterizedTest(name = "{0} {1}")
	@MethodSource
	public void test(AggregateOperator operator, DataSet sample, double result)
	{
		VarIDOperand operand = new VarIDOperand("operand");
		Map<String, DataSet> map = new HashMap<>();
		map.put("operand", sample);
		TransformationScheme session = TestUtils.mockSession(map);

		AggregateTransformation at = new AggregateTransformation(operator, operand, null, null);
		final VTLValueMetadata metadata = at.getMetadata(session);
		assertTrue(metadata instanceof DataSetMetadata, "Result structure is dataset: " + metadata);
		assertEquals(((DataSetMetadata) metadata).size(), 1, "Only one measure in " + metadata);

		final VTLValue eval = at.eval(session);
		assertTrue(eval instanceof DataSet, eval.getClass().getSimpleName() + " instanceof DataSet");
		DataSet dataset = (DataSet) eval;
		
		assertEquals(dataset.size(), 1, "Only one datapoint in result");
		DataPoint dp = dataset.stream().findAny().get();
		assertEquals(result, dp.values().iterator().next().get(), "Result of " + operator);
	}
}
