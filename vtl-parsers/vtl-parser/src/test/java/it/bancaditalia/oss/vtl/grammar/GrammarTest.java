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
package it.bancaditalia.oss.vtl.grammar;

import static java.nio.file.Files.newBufferedReader;
import static org.antlr.v4.runtime.CharStreams.fromReader;
import static org.antlr.v4.runtime.CharStreams.fromString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;

public class GrammarTest
{
	@Test
	public void positiveTest() throws Throwable
	{
		try (BufferedReader reader = newBufferedReader(Paths.get(GrammarTest.class.getResource("PositiveTests.vtl").toURI())))
		{
			Vtl parser = new Vtl(new CommonTokenStream(new VtlTokens(fromReader(reader))));
	        SyntaxErrorListener listener = new SyntaxErrorListener();
	        parser.addErrorListener(listener);
			parser.start();
			
			List<SyntaxError> errors = listener.getSyntaxErrors();
			assertTrue(errors.isEmpty(), errors::toString);
		}
	}

	@Test
	public void negativeTest() throws Throwable
	{
		try (BufferedReader reader = newBufferedReader(Paths.get(GrammarTest.class.getResource("NegativeTests.vtl").toURI())))
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
					VtlTokens lexer = new VtlTokens(fromString(buffer));
					Vtl parser = new Vtl(new CommonTokenStream(lexer));
			        SyntaxErrorListener listener = new SyntaxErrorListener();
			        lexer.addErrorListener(listener);
			        parser.addErrorListener(listener);
					parser.start();

					List<SyntaxError> errors = listener.getSyntaxErrors();
					assertFalse(errors.isEmpty(), "Expected test to fail at line " + lineCount);
					
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
		}
	}
}
