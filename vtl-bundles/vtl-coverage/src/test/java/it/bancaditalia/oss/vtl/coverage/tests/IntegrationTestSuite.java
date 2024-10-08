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
		
		try (BufferedReader dirReader = new BufferedReader(new InputStreamReader(IntegrationTestSuite.class.getResourceAsStream("../tests"), UTF_8)))
		{
			while ((testName = dirReader.readLine()) != null)
				if (!testName.endsWith(".class"))
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
