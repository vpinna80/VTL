/**
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
package it.bancaditalia.oss.vtl.it;

import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class DatasetIT {
	
	private static final DataStructureComponent<?, ?, ?> TIME_PERIOD = new DataStructureComponentImpl<>("TIME_PERIOD", Identifier.class, Domains.STRINGDS);
	private static final DataStructureComponent<?, ?, ?> OBS_VALUE = new DataStructureComponentImpl<>("OBS_VALUE", Measure.class, Domains.NUMBERDS);
	
	public static Stream<Arguments> test() {
	    return Stream.of(
	  	      Arguments.of("SDMX", "sdmx:ECB.EXR.A.USD.EUR.SP00.A", 31, "+", (DoubleBinaryOperator) (a, b) -> a + b),
		      Arguments.of("SDMX", "sdmx:ECB.EXR.A.USD.EUR.SP00.A", 31, "-", (DoubleBinaryOperator) (a, b) -> a - b),
		      Arguments.of("SDMX", "sdmx:ECB.EXR.A.USD.EUR.SP00.A", 31, "*", (DoubleBinaryOperator) (a, b) -> a * b),
		      Arguments.of("SDMX", "sdmx:ECB.EXR.A.USD.EUR.SP00.A", 31, "/", (DoubleBinaryOperator) (a, b) -> a / b),
	  	      Arguments.of("CSV", "csv:test_data/ecbexrusd.csv", 16, "+", (DoubleBinaryOperator) (a, b) -> a + b),
		      Arguments.of("CSV", "csv:test_data/ecbexrusd.csv", 16, "-", (DoubleBinaryOperator) (a, b) -> a - b),
		      Arguments.of("CSV", "csv:test_data/ecbexrusd.csv", 16, "*", (DoubleBinaryOperator) (a, b) -> a * b),
		      Arguments.of("CSV", "csv:test_data/ecbexrusd.csv", 16, "/", (DoubleBinaryOperator) (a, b) -> a / b)
	    );
	}

	@ParameterizedTest(name = "{0} operation {3}")
	@MethodSource
	public void test(String testName, String datasetName, int expectedColumns, String operatorName, DoubleBinaryOperator op) 
	{
		System.setProperty("NO_R", "true");
		
		String script = "a:='" + datasetName + "';\n"
					+ "b:=a;\n"
					+ "c:=a" + operatorName + "b;\n";

		VTLSession session = ConfigurationManager.getDefault().createSession();
		session.addStatements(script);
		session.compile();
		
		Map<String, Double> left = extract(session.resolve("a", DataSet.class), expectedColumns ,"left operand"),
				right = extract(session.resolve("b", DataSet.class), expectedColumns ,"right operand"),
				result = extract(session.resolve("c", DataSet.class), 0 ,"result");
		
		for (String timePeriod: result.keySet())
			assertEquals(left.get(timePeriod), right.get(timePeriod), 0, "Wrong operands (" + testName + " " + operatorName + ") for " + timePeriod);

		for (String timePeriod: result.keySet())
			assertEquals(op.applyAsDouble(left.get(timePeriod), right.get(timePeriod)), 
					result.get(timePeriod), 0, "Wrong result (" + testName + " " + operatorName + ") for " + timePeriod);
	}
	
	private static Map<String, Double> extract(DataSet dataset, int expectedSize, String description)
	{
		assertEquals(dataset.stream().collect(toSet()), dataset.stream().collect(toSet()), "Parallelization error on " + description);
		
		assertNotNull(dataset, "Null " + description);
		if (expectedSize > 0)
			assertEquals(expectedSize, dataset.getMetadata().size(), "Wrong number of columns for " + description);
		
		assertTrue(dataset.getMetadata().contains("TIME_PERIOD"), "Missing column TIME_PERIOD in " + description);
		assertTrue(dataset.getMetadata().contains("OBS_VALUE"), "Missing column OBS_VALUE in " + description);
		assertTrue(dataset.stream().count() == dataset.stream().map(dp -> dp.get(TIME_PERIOD)).map(StringValue.class::cast).map(StringValue::get).distinct().count(),
				"TIME_PERIOD not unique in " + description);
		
		return dataset.stream()
			.collect(toConcurrentMap(dp -> (String) dp.get(TIME_PERIOD).get(), dp -> (Double) dp.get(OBS_VALUE).get()));
			
	}
}