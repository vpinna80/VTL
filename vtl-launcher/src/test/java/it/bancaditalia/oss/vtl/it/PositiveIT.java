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

import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.xml.bind.JAXBException;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedContextException;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedTokenException;

class PositiveIT
{
	static JavaVTLEngine engine;
	
	@BeforeAll
	static void setUpBeforeClass() throws ClassNotFoundException, JAXBException, IOException
	{
		engine = new JavaVTLEngine();
	}
	
	public static Stream<Arguments> implementedTest() throws IOException
	{
		Integer testsToSkip[] = { 1, 3, 15, 16, 17, 18, 19, 20, 27, 31, 32, 33, 34, 37, 46, 47, 48, 56, 57, 60, 61, 64, 65, 68, 
				69, 70, 71, 75, 76, 77, 78, 79, 82, 83, 84, 85, 90, 91, 126, 128, 130, 140, 142, 150, 151, 155, 156, 158, 159, 
				161, 162, 163, 164, 169, 171, 173, 197, 198, 199, 200, 202, 204, 205, 210, 211, 215, 216, 217, 218, 225, 226, 
				227, 228, 247, 248, 251, 252, 254, 256, 258, 259, 260, 262, 263, 265, 268, 269, 273, 275, 277, 279, 281 };
		Set<Integer> skip = new HashSet<>(Arrays.asList(testsToSkip));

//		Integer skipAddSemicolon[] = { 2 };
//		Set<Integer> skipSemicolon = new HashSet<>(Arrays.asList(skipAddSemicolon));
		
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(PositiveIT.class.getResourceAsStream("/PositiveTests.vtl"), StandardCharsets.UTF_8)))
		{
			List<Arguments> parameters = new LinkedList<>();
			StringBuilder buffer = new StringBuilder();
			int counter = 1;
			String line;
			while ((line = reader.readLine()) != null)
			{
				if (line.isEmpty())
				{
					String statement = buffer.toString();
					buffer.setLength(0);
					if (!statement.trim().endsWith(";"))
						statement = statement + ";";
					if (!skip.contains(counter))
						parameters.add(Arguments.of(counter, statement));
					counter++;
				}
				else
					buffer.append(line).append(System.lineSeparator());
			};
			
			return parameters.stream();
		}
	}	

	@ParameterizedTest(name = "Statement {0}: {1}")
	@MethodSource
	void implementedTest(int index, String statement)
	{
		try 
		{
			engine.parseRules(statement);
		}
		catch (VTLNestedException e)
		{
			Throwable cause = e;
			while (cause.getCause() != null)
				cause = cause.getCause();
			
			if (cause instanceof VTLUnmappedContextException)
			{
				ParserRuleContext context = ((VTLUnmappedContextException) cause).getContext();
				String output = context.start.getInputStream().getText(Interval.of(context.start.getStartIndex(), context.stop.getStopIndex())).replaceAll("\\s", " ");
				fail("Not implemented: " + output, e);
			}
			else if (cause instanceof VTLUnmappedTokenException)
			{
				fail(e.getMessage(), e);
			}
			else
				throw new RuntimeException(e.getCause());
		}
		catch (VTLUnmappedContextException e)
		{
			ParserRuleContext context = e.getContext();
			String output = context.start.getInputStream().getText(Interval.of(context.start.getStartIndex(), context.stop.getStopIndex())).replaceAll("\\s", " ");
			fail("Not implemented: " + output, e);
		}
		catch (VTLUnmappedTokenException e)
		{
			fail(e.getMessage(), e);
		}
	}
}
