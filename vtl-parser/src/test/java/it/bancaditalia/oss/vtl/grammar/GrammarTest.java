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
