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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.antlr.v4.runtime.CharStreams.fromReader;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;

public class GrammarTest
{
	@Test
	public void grammarTest() throws Throwable
	{
		try (Reader reader = new InputStreamReader(GrammarTest.class.getResourceAsStream("PositiveTests.vtl"), UTF_8))
		{
			Vtl parser = new Vtl(new CommonTokenStream(new VtlTokens(fromReader(reader))));
	        SyntaxErrorListener listener = new SyntaxErrorListener();
	        parser.addErrorListener(listener);
			parser.start();
			
			List<SyntaxError> errors = listener.getSyntaxErrors();
			assertTrue(errors.isEmpty(), errors::toString);
		}
	}
}
