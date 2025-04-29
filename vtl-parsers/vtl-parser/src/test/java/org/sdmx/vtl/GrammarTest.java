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
package org.sdmx.vtl;

import static java.lang.System.lineSeparator;
import static java.nio.file.Files.newBufferedReader;
import static java.util.stream.Collectors.joining;
import static org.antlr.v4.runtime.CharStreams.fromString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.sdmx.vtl.Vtl.StartContext;

public class GrammarTest
{
	public static List<Arguments> positiveTest() throws IOException, URISyntaxException
	{
		return buildTests("PositiveTests.vtl");
	}
	
	public static List<Arguments> negativeTest() throws IOException, URISyntaxException
	{
		return buildTests("NegativeTests.vtl");
	}

	@ParameterizedTest(name = "Line {0}")
	@MethodSource
	public void positiveTest(int lineCount, String buffer, String context) throws Throwable
	{
		Entry<List<SyntaxError>, String> parseResult = doTest(buffer);
		assertTrue(parseResult.getKey().isEmpty(), parseResult.getKey()::toString);
		assertNotNull(parseResult.getValue(), "Parse tree empty");
		if (context != null)
			assertEquals(context, parseResult.getValue());
		else if (!"Start".equals(parseResult.getValue()))
		{
			lineCount -= buffer.split(System.lineSeparator()).length - 1;
			fail("Missing parse tree at line " + lineCount + ". The computed parse tree is\n" + parseResult.getValue());
		}
	}

	@ParameterizedTest(name = "Line {0}")
	@MethodSource
	public void negativeTest(int lineCount, String buffer, String context) throws Throwable
	{
		List<SyntaxError> errors = doTest(buffer).getKey();
		assertFalse(errors.isEmpty(), "Expected test to fail at line " + lineCount);
	}

	private static List<Arguments> buildTests(String file) throws IOException, URISyntaxException
	{
		List<Arguments> args = new ArrayList<>();
		
		try (BufferedReader reader = newBufferedReader(Paths.get(GrammarTest.class.getResource(file).toURI())))
		{
			String line;
			int lineCount = 0;
			String buffer = "";
			while ((line = reader.readLine()) != null)
			{
				if (!line.trim().isEmpty())
				{
					buffer += line + System.lineSeparator();
					lineCount++;
				}
				else if (!buffer.matches("(\r?\n| )*"))
				{
					String context = null;
					String trimmed = buffer.stripLeading();
					if (trimmed.startsWith("## "))
					{
						String[] split = trimmed.split(System.lineSeparator(), 2);
						context = split[0].substring(3);
						buffer = buffer.substring(0, buffer.length() - trimmed.length()) + lineSeparator() + split[1];
					}
					
					args.add(arguments(lineCount, buffer, context));
					
					lineCount++;
					char[] emptyLines = new char[lineCount];
					Arrays.fill(emptyLines, '\n');
					buffer = new String(emptyLines);
				}
				else
				{
					lineCount++;
					buffer += System.lineSeparator();
				}
			}

			// Check last example too
			String context = null;
			String trimmed = buffer.stripLeading();
			if (trimmed.startsWith("## "))
			{
				String[] split = trimmed.split(System.lineSeparator(), 2);
				context = split[0].substring(3);
				buffer = buffer.substring(0, buffer.length() - trimmed.length()) + lineSeparator() + split[1];
			}
			
			args.add(arguments(lineCount, buffer, context));
		}
		
		return args;
	}
	
	private static Entry<List<SyntaxError>, String> doTest(String buffer) 
	{
		VtlTokens lexer = new VtlTokens(fromString(buffer));
		Vtl parser = new Vtl(new CommonTokenStream(lexer));
		SyntaxErrorListener listener = new SyntaxErrorListener();
		lexer.addErrorListener(listener);
		parser.addErrorListener(listener);
		StartContext start = parser.start();
		
		List<SyntaxError> errors = listener.getSyntaxErrors();
		return new SimpleEntry<>(errors, buildTree(start));
	}

	private static String buildTree(ParserRuleContext ctx)
	{
		List<ParserRuleContext> children  = ctx.getRuleContexts(ParserRuleContext.class);
		String simpleName = ctx.getClass().getSimpleName().replace("Context", "");
		
		if (children.isEmpty())
			return simpleName;
		else
			return children.stream()
				.map(GrammarTest::buildTree)
				.collect(joining(" ", simpleName + "[", "]"));
	}
}
