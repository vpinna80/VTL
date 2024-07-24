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
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
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
	private static final Path TEST_ROOT;
	private static final Set<Integer> CATEG_TO_DO = Set.of();
	private static final Set<String> TESTS_TO_DO = Set.of();
	private static final Semaphore LOCK = new Semaphore(1);
	
	static 
	{
		try
		{
			TEST_ROOT = Paths.get(IntegrationTestSuite.class.getResource("../tests").toURI());
		}
		catch (URISyntaxException e)
		{
			throw new ExceptionInInitializerError(e);
		}
	}
	
	public static Stream<Arguments> test() throws IOException, URISyntaxException
	{
		METADATA_REPOSITORY.setValue(JsonMetadataRepository.class);
		
		List<Arguments> tests = new ArrayList<>();
		String category = null;
		String operator = null;
		String test = null;
		StringBuilder testCode = new StringBuilder();
		
		try (BufferedReader dirReader = new BufferedReader(new InputStreamReader(IntegrationTestSuite.class.getResourceAsStream("../tests"), UTF_8)))
		{
			while ((category = dirReader.readLine()) != null)
				if (!category.endsWith(".class") && (CATEG_TO_DO.isEmpty() || CATEG_TO_DO.contains(Integer.parseInt(category.substring(0, 2)))))
					try (BufferedReader catReader = new BufferedReader(new InputStreamReader(IntegrationTestSuite.class.getResourceAsStream("../tests/" + category), UTF_8)))
					{
						while ((operator = catReader.readLine()) != null)
							if (TESTS_TO_DO.isEmpty() || TESTS_TO_DO.contains(operator))
								try (BufferedReader opReader = new BufferedReader(new InputStreamReader(IntegrationTestSuite.class.getResourceAsStream("../tests/" + category + "/" + operator), UTF_8)))
								{
									while ((test = opReader.readLine()) != null)
										if (test.endsWith(".vtl"))
										{
											Matcher matcher = Pattern.compile("([0-9])").matcher(test);
											matcher.find();
											String number = matcher.group(1);
											try (BufferedReader testReader = Files.newBufferedReader(TEST_ROOT.resolve(category).resolve(operator).resolve("ex_" + number + ".vtl")))
											{
												String testLine;
												int headerLines = 20;
												while ((testLine = testReader.readLine()) != null)
												{
													if (--headerLines > 0)
														continue;
													testCode.append(testLine).append(System.lineSeparator());
												}
	
												tests.add(Arguments.of(category, operator, number, testCode.toString()));
												testCode.setLength(0);
											}
										}
								}
					}
		}
		catch (Exception e)
		{
			System.err.println(category + "/" + operator + "/" + test);
			throw e;
		}

		return tests.parallelStream();
	}

	@RepeatedParameterizedTest(value = 5, name = "{1} test {2} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("test")
	public void test(String categ, String operator, String number, String testCode) throws IOException, URISyntaxException, InterruptedException
	{
		LOCK.acquire();
		VTLSession session;
		try 
		{
			ENVIRONMENT_IMPLEMENTATION.setValues(CSVPathEnvironment.class, WorkspaceImpl.class);
			VTL_CSV_ENVIRONMENT_SEARCH_PATH.setValues(TEST_ROOT.resolve(categ).resolve(operator).toString());
			JSON_METADATA_URL.setValue(IntegrationTestSuite.class.getResource(categ + "/" + operator + "/ex_" + number + ".json").toString());
			session = ConfigurationManagerFactory.newManager().createSession(testCode);
		}
		finally
		{
			LOCK.release();
		}
		
		doTest(number, session);
	}

	@RepeatedParameterizedTest(value = 5, name = "{1} test {2} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("test")
	public void testSpark(String categ, String operator, String number,  String testCode) throws IOException, URISyntaxException, InterruptedException
	{
		LOCK.acquire();
		VTLSession session;
		try 
		{
			ENVIRONMENT_IMPLEMENTATION.setValues(SparkEnvironment.class, WorkspaceImpl.class);
			VTL_SPARK_SEARCH_PATH.setValues(TEST_ROOT.resolve(categ).resolve(operator).toString());
			VTL_SPARK_UI_ENABLED.setValue(false);
			JSON_METADATA_URL.setValue(IntegrationTestSuite.class.getResource(categ + "/" + operator + "/ex_" + number + "-spark.json").toString());
			session = ConfigurationManagerFactory.newManager().createSession(testCode);
		}
		finally
		{
			LOCK.release();
		}

		doTest(number, session);
	}

	private void doTest(String number, VTLSession session)
	{
		session.compile();
		
		DataSet expected = session.resolve("ex_" + number, DataSet.class);
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

		for (DataPoint dp: resDPs)
			assertThat(dp, anyOf(expectedDPs.stream().map(Matchers::equalTo).collect(toList())));
		for (DataPoint dp: expectedDPs)
			assertThat(dp, anyOf(resDPs.stream().map(Matchers::equalTo).collect(toList())));
	}
}
