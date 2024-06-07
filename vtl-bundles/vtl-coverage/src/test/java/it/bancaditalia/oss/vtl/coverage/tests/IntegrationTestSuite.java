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
package it.bancaditalia.oss.vtl.coverage.tests;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment.VTL_CSV_ENVIRONMENT_SEARCH_PATH;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment.VTL_SPARK_SEARCH_PATH;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment.VTL_SPARK_UI_ENABLED;
import static it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository.JSON_METADATA_URL;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment;
import it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl;
import it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment;
import it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class IntegrationTestSuite
{
	static {
		System.setProperty("vtl.sequential", "true");
	}

	private static Path root;
	
	public static Stream<Arguments> test() throws IOException, URISyntaxException
	{
		root = Paths.get(IntegrationTestSuite.class.getResource("../tests").toURI());

		METADATA_REPOSITORY.setValue(JsonMetadataRepository.class);
		
		List<Arguments> tests = new ArrayList<>();
		String testName;
		StringBuilder testCode = new StringBuilder();

		Set<String> skipped = Stream.of(
		"abs",
		"absolute_value_1",
		"absolute_value_2",
		"aggr",
		"aggr_1",
		"aggr_2",
		"and_1",
		"and_2",
		"and_or",
		"average",
		"basic_arithmetic_1",
		"basic_arithmetic_2",
		"between_1",
		"between_2",
		"boolean_and_1",
		"boolean_and_2",
		"boolean_not_1",
		"boolean_or_1",
		"boolean_or_2",
		"boolean_xor_1",
		"boolean_xor_2",
		"calc",
		"calc_1",
		"case_conversion_1",
		"case_conversion_2",
		"case_conversion_3",
		"ceil",
		"ceil_1",
		"ceil_floor_1",
		"count",
		"div",
		"drop",
		"element_of_1",
		"element_of_2",
		"element_of_3",
		"equal",
		"equal_1",
		"equal_2",
		"exists_in_1",
		"exists_in_2",
		"exists_in_3",
		"exp",
		"exp_2",
		"fill_timeseries_1",
		"fill_timeseries_2",
		"filter",
		"filter_1",
		"filter_2",
		"first_value_1",
		"first_value_2",
		"floor",
		"floor_1",
		"flow_to_stock_1",
		"flow_to_stock_2",
		"greater",
		"greatereq",
		"greater_than_1",
		"greater_than_2",
		"if-then-else_1",
		"if-then-else_2",
		"intersection",
		"is_null_1",
		"is_null_2",
		"keep",
		"keep_1",
		"lag_1",
		"lag_2",
		"last_value_1",
		"last_value_2",
		"lead_1",
		"lead_2",
		"lesser",
		"lessereq",
		"less_than_1",
		"ln",
		"ln_1",
		"ln_2",
		"log_1",
		"log_2",
		"match_characters_1",
		"match_characters_2",
		"max",
		"median",
		"membership_1",
		"min",
		"minus",
		"mod",
		"mod_1",
		"mod_2",
		"mult",
		"notequal",
		"not_1",
		"not_2",
		"not_equal_1",
		"not_equal_2",
		"not_xor",
		"nvl_1",
		"nvl_2",
		"nvl_3",
		"op_with_calculated_scalar",
		"or_1",
		"or_2",
		"parentheses_1",
		"parentheses_2",
		"pattern_location_1",
		"pattern_location_2",
		"pattern_replacement_1",
		"pattern_replacement_2",
		"pattern_replacement_3",
		"plus",
		"pow",
		"power_1",
		"power_2",
		"rank_1",
		"ratio_to_report_1",
		"ratio_to_report_2",
		"rename",
		"rename_1",
		"round",
		"round_1",
		"round_2",
		"self_defined_operator1",
		"self_defined_operator2",
		"set_difference_1",
		"sqrt",
		"sqrt_1",
		"sqrt_2",
		"standard_deviation_pop",
		"standard_deviation_samp",
		"stock_to_flow_1",
		"stock_to_flow_2",
		"string_concatenation_1",
		"string_concatenation_2",
		"string_concatenation_3",
		"string_trim_1",
		"string_trim_2",
		"string_trim_3",
		"string_trim_4",
		"sub",
		"substring_extraction_1",
		"substring_extraction_2",
		"sub_1",
		"sum",
		"sum_1",
		"symmetric_difference_1",
		"timeshift_1",
		"timeshift_2",
		"trunc_1",
		"trunc_2",
		"uminus",
		"union_1",
		"union_2",
		"uplus",
		"var_pop",
		"var_samp",
		"xor_1",
		"xor_2"
		).collect(Collectors.toSet());
		
		try (BufferedReader dirReader = new BufferedReader(new InputStreamReader(IntegrationTestSuite.class.getResourceAsStream("../tests"), UTF_8)))
		{
			while ((testName = dirReader.readLine()) != null)
				if (!testName.endsWith(".class")/* && !skipped.contains(testName) */)
				{
					try (BufferedReader testReader = Files.newBufferedReader(root.resolve(testName).resolve(testName + ".vtl")))
					{
						String testLine;
						int headerLines = 20;
						while ((testLine = testReader.readLine()) != null)
						{
							if (--headerLines > 0)
								continue;
							testCode.append(testLine).append(System.lineSeparator());
						}
					}
					tests.add(Arguments.of(testName, testCode.toString()));
					testCode.setLength(0);
				}
		}

		return tests.stream();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource
	public synchronized void test(final String testName, final String testCode) throws IOException, URISyntaxException
	{
		System.out.println("------------------------------------------------------------------------------------------------");
		System.out.println("                                        " + testName);
		System.out.println("------------------------------------------------------------------------------------------------");
		System.out.println();
		System.out.println(testCode);
		System.out.println("------------------------------------------------------------------------------------------------");

		ENVIRONMENT_IMPLEMENTATION.setValues(CSVPathEnvironment.class, WorkspaceImpl.class);
		VTL_CSV_ENVIRONMENT_SEARCH_PATH.setValues(root.resolve(testName).toString());
		JSON_METADATA_URL.setValue(IntegrationTestSuite.class.getResource(testName + "/" + testName + ".json").toString());
		VTLSession session = ConfigurationManagerFactory.newManager().createSession(testCode);
		
		session.compile();
		
		DataSet expected = session.resolve("expected", DataSet.class);
		DataSet result = session.resolve("test_result", DataSet.class);
		
		for (DataStructureComponent<?, ?, ?> comp: expected.getMetadata())
			assertTrue(result.getMetadata().contains(comp), "Expected component " + comp + " is missing in " + result.getMetadata());
		for (DataStructureComponent<?, ?, ?> comp: result.getMetadata())
			assertTrue(expected.getMetadata().contains(comp), "Unexpected component " + comp + " in result.");
		
		List<DataPoint> resDPs;
		List<DataPoint> expectedDPs;
		try (Stream<DataPoint> resStream = result.stream(); Stream<DataPoint> expStream = expected.stream())
		{
			resDPs = resStream.collect(toList());
			expectedDPs = expStream.collect(toList());
		}
		
		System.out.println("Expected:");
		expectedDPs.forEach(System.out::println);
		System.out.println("Actual:");
		resDPs.forEach(System.out::println);
		
		for (DataPoint dp: resDPs)
			assertThat(dp, anyOf(expectedDPs.stream().map(Matchers::equalTo).collect(toList())));
		for (DataPoint dp: expectedDPs)
			assertThat(dp, anyOf(resDPs.stream().map(Matchers::equalTo).collect(toList())));
	}

	@ParameterizedTest(name = "{0} - Spark")
	@MethodSource("test")
	public synchronized void testSpark(final String testName, final String testCode) throws IOException, URISyntaxException
	{
		System.out.println("------------------------------------------------------------------------------------------------");
		System.out.println("                                        SPARK -- " + testName);
		System.out.println("------------------------------------------------------------------------------------------------");
		System.out.println();
		System.out.println(testCode);
		System.out.println("------------------------------------------------------------------------------------------------");

		ENVIRONMENT_IMPLEMENTATION.setValues(SparkEnvironment.class, WorkspaceImpl.class);
		VTL_SPARK_SEARCH_PATH.setValues(root.resolve(testName).toString());
		VTL_SPARK_UI_ENABLED.setValue(false);
		JSON_METADATA_URL.setValue(IntegrationTestSuite.class.getResource(testName + "/" + testName + "-spark.json").toString());
		VTLSession session = ConfigurationManagerFactory.newManager().createSession(testCode);
		
		session.compile();
		
		DataSet expected = session.resolve("expected", DataSet.class);
		DataSet result = session.resolve("test_result", DataSet.class);
		
		for (DataStructureComponent<?, ?, ?> comp: expected.getMetadata())
			assertTrue(result.getMetadata().contains(comp), "Expected component " + comp + " is missing in " + result.getMetadata());
		for (DataStructureComponent<?, ?, ?> comp: result.getMetadata())
			assertTrue(expected.getMetadata().contains(comp), "Unexpected component " + comp + " in result.");
		
		List<DataPoint> resDPs;
		List<DataPoint> expectedDPs;
		try (Stream<DataPoint> resStream = result.stream(); Stream<DataPoint> expStream = expected.stream())
		{
			resDPs = resStream.collect(toList());
			expectedDPs = expStream.collect(toList());
		}
		
		System.out.println("Expected:");
		expectedDPs.forEach(System.out::println);
		System.out.println("Actual:");
		resDPs.forEach(System.out::println);

		for (DataPoint dp: resDPs)
			assertThat(dp, anyOf(expectedDPs.stream().map(Matchers::equalTo).collect(toList())));
		for (DataPoint dp: expectedDPs)
			assertThat(dp, anyOf(resDPs.stream().map(Matchers::equalTo).collect(toList())));
	}
}
