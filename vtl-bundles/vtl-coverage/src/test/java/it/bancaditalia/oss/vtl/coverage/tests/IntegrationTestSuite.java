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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.coverage.tests.IntegrationTestSuite.TestType.E;
import static it.bancaditalia.oss.vtl.coverage.tests.IntegrationTestSuite.TestType.ES;
import static it.bancaditalia.oss.vtl.coverage.tests.IntegrationTestSuite.TestType.T;
import static it.bancaditalia.oss.vtl.coverage.tests.IntegrationTestSuite.TestType.TS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.coverage.utils.RepeatedParameterizedTest;
import it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine;
import it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment;
import it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl;
import it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment;
import it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository;
import it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class IntegrationTestSuite
{
	public static enum TestType
	{
		T, E, TS, ES;
	}
	
	private static final int REPETITIONS = 1;
	private static final Path TEST_ROOT;
	private static final Path EXAMPLES_ROOT;
	private static final Set<TestType> TO_SKIP = Set.of();
	
	static 
	{
		try
		{
			System.getProperty("vtl.double.ulps.epsilon", "1000000000");
			TEST_ROOT = Paths.get(IntegrationTestSuite.class.getResource("../tests").toURI());
			EXAMPLES_ROOT = Paths.get(IntegrationTestSuite.class.getResource("../examples").toURI());
			METADATA_REPOSITORY.setValue(JsonMetadataRepository.class);
		}
		catch (URISyntaxException e)
		{
			throw new ExceptionInInitializerError(e);
		}
	}
	
	public static Stream<Arguments> test() throws IOException
	{
		return getStream(TEST_ROOT);
	}

	public static Stream<Arguments> examples() throws IOException
	{
		return getStream(EXAMPLES_ROOT);
	}

	private static Stream<Arguments> getStream(Path root) throws IOException
	{
		StringBuilder testCode = new StringBuilder();
		List<Arguments> tests = new ArrayList<>();

		for (Path category: Files.newDirectoryStream(root, Files::isDirectory))
			for (Path operator: Files.newDirectoryStream(category, Files::isDirectory))
				for (Path test: Files.newDirectoryStream(operator, "*.vtl"))
				{
					Matcher matcher = Pattern.compile("([0-9])").matcher(test.getFileName().toString());
					if (!matcher.find())
						throw new IllegalStateException(test.toString());
					String number = matcher.group(1);
					try (BufferedReader testReader = Files.newBufferedReader(test))
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

		return tests.parallelStream();
	}
	
	@RepeatedParameterizedTest(value = REPETITIONS, name = "{1} test {2} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("test")
	public void test(Path categ, Path operator, String number, String testCode) throws Throwable 
	{
		if (TO_SKIP.contains(T))
			return;
		
		URL jsonURL = operator.resolve(String.format("ex_%s.json", number)).toUri().toURL();
		VTLSession session = new VTLSessionImpl(testCode, new JsonMetadataRepository(jsonURL), new JavaVTLEngine(), 
				List.of(new CSVPathEnvironment(List.of(operator)), new WorkspaceImpl()));
		doTest(number, session);
	}

	@RepeatedParameterizedTest(value = REPETITIONS, name = "{0}: Test {2} of {1} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("examples")
	public void examples(Path categ, Path operator, String number, String testCode) throws Throwable
	{
		if (TO_SKIP.contains(E))
			return;
		
		URL jsonURL = operator.resolve(String.format("ex_%s.json", number)).toUri().toURL();
		VTLSession session = new VTLSessionImpl(testCode, new JsonMetadataRepository(jsonURL), new JavaVTLEngine(), 
				List.of(new CSVPathEnvironment(List.of(operator)), new WorkspaceImpl()));
		doTest(number, session);
	}

	@RepeatedParameterizedTest(value = REPETITIONS, name = "{1} test {2} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("test")
	public void testSpark(Path categ, Path operator, String number, String testCode) throws Throwable 
	{
		if (TO_SKIP.contains(TS))
			return;
		
		URL jsonURL = operator.resolve(String.format("ex_%s-spark.json", number)).toUri().toURL();
		VTLSession session = new VTLSessionImpl(testCode, new JsonMetadataRepository(jsonURL), new JavaVTLEngine(), 
				List.of(new SparkEnvironment(List.of(operator)), new WorkspaceImpl()));
		doTest(number, session);
	}

	@RepeatedParameterizedTest(value = REPETITIONS, name = "{0}: Test {2} of {1} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("examples")
	public void examplesSpark(Path categ, Path operator, String number, String testCode) throws Throwable
	{
		if (TO_SKIP.contains(ES))
			return;
		
		URL jsonURL = operator.resolve(String.format("ex_%s-spark.json", number)).toUri().toURL();
		VTLSession session = new VTLSessionImpl(testCode, new JsonMetadataRepository(jsonURL), new JavaVTLEngine(), 
				List.of(new SparkEnvironment(List.of(operator)), new WorkspaceImpl()));
		doTest(number, session);
	}

	public static void doTest(String number, VTLSession session)
	{
		session.compile();
		
		DataSet expected = session.resolve(VTLAliasImpl.of("ex_" + number), DataSet.class);
		DataSet result = session.resolve(VTLAliasImpl.of("ds_r"), DataSet.class);
		
		for (DataStructureComponent<?, ?, ?> comp: expected.getMetadata())
			assertTrue(result.getMetadata().contains(comp), "In " + session.getOriginalCode() + "Expected component " + comp + " is missing from result structure " + result.getMetadata());
		for (DataStructureComponent<?, ?, ?> comp: result.getMetadata())
			assertTrue(expected.getMetadata().contains(comp), "In " + session.getOriginalCode() + "Unexpected component " + comp + " not declared in structure " + expected.getMetadata());
		
		List<DataPoint> resDPs, expectedDPs;
		try (Stream<DataPoint> resStream = result.stream(); Stream<DataPoint> expStream = expected.stream())
		{
			resDPs = resStream.collect(toList());
			expectedDPs = expStream.collect(toList());
		}
		
		checkDPs(resDPs, expectedDPs, "Unexpected datapoint found");
		checkDPs(expectedDPs, resDPs, "Expected datapoint not found");
	}
	
	private static void checkDPs(List<DataPoint> toCheck, List<DataPoint> against, String prefix)
	{
		for (DataPoint dpr: toCheck)
		{
			boolean found = false;
			for (DataPoint dpe: against)
				if (!found && dpe.equals(dpr))
					found = true;
				
			if (!found)
			{
				StringWriter writer = new StringWriter();
				PrintWriter pr = new PrintWriter(writer);
				pr.println(prefix + "\n" + dpr + "\n--------------------------------");
				for (DataPoint dpe: against)
					pr.println(dpe);
				fail(writer.toString());
			}
		}
	}
}
