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
package it.bancaditalia.oss.vtl.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.session.VTLSessionHandler;

public class FilterIT
{
	@FunctionalInterface
	private interface TestBody extends BiConsumer<Map<String, List<Object>>, Map<String, List<Object>>>
	{
		
	}

	private static Stream<Arguments> test() {
	    return Stream.of(
	  	      Arguments.of("SDMX Identifiers", "sdmx:ECB.EXR.A.USD+GBP.EUR.SP00.A", "CURRENCY=\"USD\"", (TestBody) FilterIT::checkIdentifiers),
	  	      Arguments.of("SDMX Measures", "sdmx:ECB.EXR.A.USD+GBP.EUR.SP00.A", "OBS_VALUE=0.609477647058823", (TestBody) FilterIT::checkMeasures),
	  	      Arguments.of("SDMX Attributes", "sdmx:ECB.EXR.A.USD+GBP.EUR.SP00.A", "TITLE=\"UK pound sterling/Euro\"", (TestBody) FilterIT::checkAttributes),
	  	      Arguments.of("CSV Identifiers", "csv:test_data/ecbexr.csv", "CURRENCY=\"USD\"", (TestBody) FilterIT::checkIdentifiers),
	  	      Arguments.of("CSV Measures", "csv:test_data/ecbexr.csv", "OBS_VALUE=0.609477647058823", (TestBody) FilterIT::checkMeasures),
	  	      Arguments.of("CSV Attributes", "csv:test_data/ecbexr.csv", "TITLE=\"UK pound sterling/Euro\"", (TestBody) FilterIT::checkAttributes)
	    );
	}

	@ParameterizedTest(name="{0}")
	@MethodSource
	public void test(String testName, String datasetName, String filterBody, BiConsumer<Map<String, List<Object>>, Map<String, List<Object>>> testBody)
	{
		System.setProperty("NO_R", "true");

		String script = "a:='" + datasetName + "';\n"
				+ "b:=a[filter " + filterBody + "];\n";

		VTLSessionHandler.addStatements(testName, script);
		VTLSessionHandler.compile(testName);

		Map<String, List<Object>> left = VTLSessionHandler.evalNode(testName, "b");
		Map<String, List<Object>> right = VTLSessionHandler.evalNode(testName, "b");
		testBody.accept(left, right);
	}

	private static void checkIdentifiers(Map<String, List<Object>> left, Map<String, List<Object>> right)
	{
		assertNotNull(left, "Null result");
		assertEquals(16, left.size(), "Empty result"); // two dimensions and 14 attributes
		assertNotNull(left.get("OBS_VALUE"), "Wrong Structure result");
		List<Object> term1 = left.get("OBS_VALUE");
		List<Object> time1 = left.get("TIME_PERIOD");
		List<Object> curr1 = left.get("CURRENCY");
		Map<String, Object> map1 = new HashMap<>();
		for (int i = 0; i < curr1.size(); i++)
		{
			String currency = (String) curr1.get(i);
			if (currency.equals("USD"))
				map1.put((String) time1.get(i), (Double) term1.get(i));
		}

		assertNotNull(right, "Null result");
		assertEquals(16, right.size(), "Empty result"); // same dimensions and attributes
		assertNotNull(right.get("OBS_VALUE"), "Wrong Structure result");
		List<Object> term2 = right.get("OBS_VALUE");
		List<Object> time2 = right.get("TIME_PERIOD");
		List<Object> curr2 = right.get("CURRENCY");
		Map<String, Object> map2 = new HashMap<>();
		for (int i = 0; i < curr2.size(); i++)
		{
			String currency = (String) curr2.get(i);
			assertEquals("USD", currency, "Wrong Currency filtered");
			map2.put((String) time2.get(i), (Double) term2.get(i));
		}

		assertEquals(map1.size(), map2.size(), "Wrong number of Rows");

		for (Entry<String, Object> entry: map1.entrySet())
			assertEquals(entry.getValue(), map2.get(entry.getKey()), "Wrong Value Result");
	}

	private static void checkAttributes(Map<String, List<Object>> left, Map<String, List<Object>> right)
	{
		assertNotNull(left, "Null result");
		assertEquals(16, left.size(), "Empty result"); // two dimensions and 14 attributes
		assertNotNull(left.get("OBS_VALUE"), "Wrong Structure result");
		List<Object> term1 = left.get("OBS_VALUE");
		List<Object> time1 = left.get("TIME_PERIOD");
		List<Object> curr1 = left.get("CURRENCY");
		Map<String, Object> map1 = new HashMap<>();
		for (int i = 0; i < curr1.size(); i++)
		{
			String currency = (String) curr1.get(i);
			if (currency.equals("GBP"))
				map1.put((String) time1.get(i), (Double) term1.get(i));
		}

		assertNotNull(right, "Null result");
		assertEquals(16, right.size(), "Empty result"); // same dimensions and attributes
		assertNotNull(right.get("OBS_VALUE"), "Wrong Structure result");
		List<Object> term2 = right.get("OBS_VALUE");
		List<Object> time2 = right.get("TIME_PERIOD");
		List<Object> curr2 = right.get("CURRENCY");
		Map<String, Object> map2 = new HashMap<>();
		for (int i = 0; i < curr2.size(); i++)
		{
			String currency = (String) curr2.get(i);
			assertEquals("GBP", currency, "Wrong Currency filtered");
			map2.put((String) time2.get(i), (Double) term2.get(i));
		}

		assertEquals(map1.size(), map2.size(), "Wrong number of Rows");

		for (Entry<String, Object> entry: map1.entrySet())
			assertEquals(entry.getValue(), map2.get(entry.getKey()), "Wrong Value Result");
	}

	private static void checkMeasures(Map<String, List<Object>> left, Map<String, List<Object>> right)
	{
		assertNotNull(left, "Null result");
		assertEquals(16, left.size(), "Empty result"); // two dimensions and 14 attributes
		assertNotNull(left.get("OBS_VALUE"), "Wrong Structure result");
		List<Object> term1 = left.get("OBS_VALUE");
		List<Object> time1 = left.get("TIME_PERIOD");
		Map<String, Object> map1 = new HashMap<>();
		for (int i = 0; i < term1.size(); i++)
		{
			Double value = (Double) term1.get(i);
			if (value == 0.609477647058823)
				map1.put((String) time1.get(i), (Double) term1.get(i));
		}

		assertNotNull(right, "Null result");
		assertEquals(16, right.size(), "Empty result"); // same dimensions and attributes
		assertNotNull(right.get("OBS_VALUE"), "Wrong Structure result");
		List<Object> term2 = right.get("OBS_VALUE");
		List<Object> time2 = right.get("TIME_PERIOD");

		assertEquals(1, term2.size(), "Wrong number of Rows");
		assertTrue(map1.keySet().contains(time2.get(0)), "Wrong Value Time");
		assertEquals((Double) map1.get(time2.get(0)), (Double) term2.get(0), 0, "Wrong Value Result");
	}
}
