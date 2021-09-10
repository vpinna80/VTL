package it.bancaditalia.oss.vtl.coverage.tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLValue;

public class IntegrationTestSuite
{
	public static Stream<Arguments> test() throws IOException, URISyntaxException
	{
		System.setProperty("vtl.sequential", "true");
		URL root = IntegrationTestSuite.class.getResource("vtl");
		Pattern pattern = Pattern.compile("'csv:([^']+)'");
		List<Arguments> tests = new ArrayList<>();
		try (BufferedReader dirReader = new BufferedReader(new InputStreamReader(IntegrationTestSuite.class.getResourceAsStream("vtl"), StandardCharsets.UTF_8)))
		{
			String testName;
			StringBuilder testCode = new StringBuilder();
			StringBuilder parsedLine = new StringBuilder();
			while ((testName = dirReader.readLine()) != null)
			{
				try (BufferedReader testReader = new BufferedReader(new InputStreamReader(new URL(root, "vtl/" + testName).openStream(), StandardCharsets.UTF_8)))
				{
					String testLine;
					int headerLines = 20;
					while ((testLine = testReader.readLine()) != null)
					{
						if (--headerLines > 0)
							continue;
						Matcher matcher = pattern.matcher(testLine);
						int start = 0;
						while (matcher.find())
						{
							parsedLine.append(testLine.substring(start, matcher.start(1)))
								.append(new URL(root, "data/" + matcher.group(1)).toString());
							start = matcher.end(1);
						}
						testCode.append(parsedLine.append(testLine.substring(start)).toString())
								.append(System.lineSeparator());
						parsedLine.setLength(0);
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
	public void test(String testName, String testCode)
	{
		if (!"symmetric_difference_2.vtl".equals(testName))
			return;
		
		VTLSessionImpl session = new VTLSessionImpl();
		session.addStatements(testCode);
		session.compile();
		session.resolve("test_result").toString();
	}
}
