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
import static it.bancaditalia.oss.vtl.coverage.tests.CoverageSuiteTests.TestType.E;
import static it.bancaditalia.oss.vtl.coverage.tests.CoverageSuiteTests.TestType.ES;
import static it.bancaditalia.oss.vtl.coverage.tests.CoverageSuiteTests.TestType.T;
import static it.bancaditalia.oss.vtl.coverage.tests.CoverageSuiteTests.TestType.TS;
import static java.lang.Integer.compare;
import static java.lang.System.lineSeparator;
import static java.nio.file.Files.newBufferedReader;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.coverage.utils.RepeatedParameterizedTest;
import it.bancaditalia.oss.vtl.coverage.utils.TestEnvironment;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine;
import it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment;
import it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository;
import it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.VTLSession;
import jakarta.xml.bind.JAXBException;

public class CoverageSuiteTests
{
	public static enum TestType
	{
		T, E, TS, ES
	}
	
	private static final int REPETITIONS = 5;
	private static final Path TEST_ROOT;
	private static final Path EXAMPLES_ROOT;
	private static final Set<TestType> TO_RUN = Set.of(T, E, TS, ES);
	private static final Set<String> SKIP_OPS = Set.of("Random", "Duration to number days", "Number days to duration");
	private static final Engine ENGINE;
	private static final boolean TOTAL_REPORT = true;
	
	static 
	{
		try
		{
			System.setProperty("spark.sql.shuffle.partitions", "5");
			System.setProperty("spark.shuffle.compress", "false");
			System.setProperty("spark.shuffle.spill.compress", "false");
			System.setProperty("spark.broadcast.compress", "false");
			System.setProperty("spark.kryo.registrationRequired", "true");
			System.setProperty("spark.kryoserializer.buffer", "1m");
			System.setProperty("spark.memory.fraction", "0.7");
			System.setProperty("spark.memory.storageFraction", "0.2");
			System.setProperty("spark.cleaner.periodicGC.interval", "30s");
			
			System.setProperty("vtl.double.epsilon", "5");
			TEST_ROOT = Paths.get(CoverageSuiteTests.class.getResource("../tests").toURI());
			EXAMPLES_ROOT = Paths.get(CoverageSuiteTests.class.getResource("../examples").toURI());
			METADATA_REPOSITORY.setValue(JsonMetadataRepository.class);
			ENGINE = new JavaVTLEngine();
		}
		catch (NoClassDefFoundError | URISyntaxException | ClassNotFoundException | JAXBException | IOException | NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e)
		{
			throw new ExceptionInInitializerError(e);
		}
	}
	
	public static List<Arguments> test() throws IOException
	{
		return getStream(TEST_ROOT, ".");
	}

	public static List<Arguments> examples() throws IOException
	{
		return getStream(EXAMPLES_ROOT, "examples");
	}

	private static List<Arguments> getStream(Path root, String suffix) throws IOException
	{
		StringBuilder testCode = new StringBuilder();
		List<Arguments> tests = new ArrayList<>();

		for (Path category: newDirectoryStream(root, Files::isDirectory))
			for (Path operator: newDirectoryStream(category, Files::isDirectory))
				for (Path test: newDirectoryStream(operator.resolve(suffix), "*.vtl"))
				{
					Matcher matcher = Pattern.compile("([0-9])").matcher(test.getFileName().toString());
					if (!matcher.find())
						throw new IllegalStateException(test.toString());
					String number = matcher.group(1);
					try (BufferedReader testReader = newBufferedReader(test))
					{
						String testLine;
						int headerLines = 20;
						while ((testLine = testReader.readLine()) != null)
						{
							if (--headerLines > 0)
								continue;
							testCode.append(testLine).append(lineSeparator());
						}

						tests.add(Arguments.of(category, operator, number, testCode.toString()));
						testCode.setLength(0);
					}
				}
		
		return tests;
	}
	
	private static JsonMetadataRepository getJsonMetaRepo(URL jsonURL) throws IOException
	{
		return new JsonMetadataRepository(jsonURL, ENGINE);
	}
	
	@RepeatedParameterizedTest(value = REPETITIONS, name = "{1} test {2} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("test")
	public void test(Path categ, Path operator, String number, String testCode) throws Throwable 
	{
		assumeTrue(TO_RUN.contains(T));
		assumeFalse(SKIP_OPS.contains(operator.getFileName().toString()));
		
		URL jsonURL = operator.resolve(String.format("ex_%s.json", number)).toUri().toURL();
		VTLSession session = new VTLSessionImpl(testCode, getJsonMetaRepo(jsonURL), ENGINE, 
				List.of(new TestEnvironment(), new CSVPathEnvironment(List.of(operator))), getMockWorkspace());
		doTest(number, session);
	}

	@RepeatedParameterizedTest(value = REPETITIONS, name = "{0}: Test {2} of {1} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("examples")
	public void examples(Path categ, Path operator, String number, String testCode) throws Throwable
	{
		assumeTrue(TO_RUN.contains(E));
		assumeFalse(SKIP_OPS.contains(operator.getFileName().toString()));
		
		URL jsonURL = operator.resolve("examples").resolve(String.format("ex_%s.json", number)).toUri().toURL();
		VTLSession session = new VTLSessionImpl(testCode, getJsonMetaRepo(jsonURL), ENGINE, 
				List.of(new TestEnvironment(), new CSVPathEnvironment(List.of(operator.resolve("examples")))), getMockWorkspace());
		doTest(number, session);
	}

	@RepeatedParameterizedTest(value = REPETITIONS, name = "{1} test {2} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("test")
	public void testSpark(Path categ, Path operator, String number, String testCode) throws Throwable 
	{
		assumeTrue(TO_RUN.contains(TS));
		assumeFalse(SKIP_OPS.contains(operator.getFileName().toString()));

		Environment sparkEnv = getSparkEnv(List.of(operator));
		URL jsonURL = operator.resolve(String.format("ex_%s-spark.json", number)).toUri().toURL();
		VTLSession session = new VTLSessionImpl(testCode, getJsonMetaRepo(jsonURL), ENGINE, 
				List.of(new TestEnvironment(), sparkEnv, new CSVPathEnvironment(List.of(operator.resolve("examples")))), getMockWorkspace());
		doTest(number, session);
	}

	@RepeatedParameterizedTest(value = REPETITIONS, name = "{0}: Test {2} of {1} rep {currentRepetition}/{totalRepetitions}")
	@MethodSource("examples")
	public void examplesSpark(Path categ, Path operator, String number, String testCode) throws Throwable
	{
		assumeTrue(TO_RUN.contains(ES));
		assumeFalse(SKIP_OPS.contains(operator.getFileName().toString()));
		
		Environment sparkEnv = getSparkEnv(List.of(operator.resolve("examples")));
		URL jsonURL = operator.resolve("examples").resolve(String.format("ex_%s-spark.json", number)).toUri().toURL();
		VTLSession session = new VTLSessionImpl(testCode, getJsonMetaRepo(jsonURL), ENGINE, 
				List.of(new TestEnvironment(), sparkEnv, new CSVPathEnvironment(List.of(operator.resolve("examples")))), getMockWorkspace());
		doTest(number, session);
	}

	public static Workspace getMockWorkspace()
	{
		Map<VTLAlias, Statement> stats = new HashMap<>();
		Workspace workspace = mock(Workspace.class, RETURNS_SMART_NULLS);
		
		when(workspace.addRule(any())).thenAnswer(ctx -> {
			Statement stat = ctx.getArgument(0);
			stats.put(stat.getAlias(), stat);
			return ctx.getMock();
		});
		
		when(workspace.getRule(any())).thenAnswer(ctx -> Optional.ofNullable(stats.get(ctx.getArgument(0))));
		
		return workspace;
	}
	
	public static Environment getSparkEnv(List<Path> paths) throws InstantiationException, IllegalAccessException, InvocationTargetException
	{
		try
		{
			Class<? extends Environment> sparkClass = Class.forName("it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment", 
					true, CoverageSuiteTests.class.getClassLoader()).asSubclass(Environment.class);
			return sparkClass.getConstructor(List.class).newInstance(paths);
		}
		catch (ClassNotFoundException | IllegalArgumentException | NoSuchMethodException | SecurityException e)
		{
			fail("Spark not available", e);
			return null;
		}
	}

	public static void doTest(String number, VTLSession session)
	{
		session.compile();
		
		VTLValue expectedV = session.resolve(VTLAliasImpl.of("ex_" + number));
		VTLValue resultV = session.resolve(VTLAliasImpl.of("ds_r"));
		
		assertTrue(expectedV.isDataSet() ^ !resultV.isDataSet(), "dataset != scalar");
		if (expectedV.isDataSet())
		{
			DataSet expected = (DataSet) expectedV;
			DataSet result = (DataSet) resultV;
			
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
			
			checkDPs(resDPs, expectedDPs, "Unexpected datapoint", "found", session.getOriginalCode());
			checkDPs(expectedDPs, resDPs, "Expected datapoint", "not found", session.getOriginalCode());
		}
		else
		{
			ScalarValue<?, ?, ?, ?> expected = (ScalarValue<?, ?, ?, ?>) expectedV;
			ScalarValue<?, ?, ?, ?> result = (ScalarValue<?, ?, ?, ?>) resultV;
			
			assertEquals(expected.getMetadata().getDomain(), result.getMetadata().getDomain());
			assertEquals(expectedV, resultV);
		}
	}
	
	private static void checkDPs(List<DataPoint> toCheck, List<DataPoint> against, String what, String verb, String code)
	{
		for (DataPoint dpr: toCheck)
		{
			boolean found = false;
			for (DataPoint dpe: against)
				if (!found && dpe.equals(dpr))
					found = true;
				
			if (!found)
			{
				Map<DataPoint, Integer> map = new TreeMap<>((dp1, dp2) -> compare(dpr.getDistance(dp1), dpr.getDistance(dp2)));
				for (DataPoint dpe: against)
					map.put(dpe, dpr.getDistance(dpe));

				StringWriter writer = new StringWriter();
				PrintWriter pr = new PrintWriter(writer);
				TreeSet<DataPoint> sorted = new TreeSet<>((dpa, dpb) -> Integer.compare(dpr.getDistance(dpa), dpr.getDistance(dpb)));
				sorted.addAll(against);
				pr.println(what + " " + verb + " for the code\n" + code + "\n--" + what + "---------------------\n" + treeify(dpr) 
						+ "\n--" + verb + " in------(" + dpr.getDistance(sorted.first()) + ")-----------");
				
				Collection<DataPoint> report = TOTAL_REPORT ? against : sorted.headSet(sorted.first(), true);
				for (DataPoint dpe: report)
					pr.println(treeify(dpe));
				pr.println("--------------------------------");
				fail(writer.toString());
			}
		}
	}

	private static TreeMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> treeify(DataPoint dpe)
	{
		TreeMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dpeTree = new TreeMap<>(DataStructureComponent::byNameAndRole);
		dpeTree.putAll(dpe);
		return dpeTree;
	}
}
