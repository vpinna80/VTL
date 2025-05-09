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
package it.bancaditalia.oss.vtl.impl.engine;

import static org.antlr.v4.runtime.atn.PredictionMode.LL_EXACT_AMBIG_DETECTION;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Stream;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.RuleNode;
import org.sdmx.vtl.VtlLexer;
import org.sdmx.vtl.VtlParser;
import org.sdmx.vtl.VtlParser.StartContext;
import org.sdmx.vtl.VtlParser.StatementContext;
import org.sdmx.vtl.VtlParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.impl.engine.statement.StatementFactory;
import jakarta.xml.bind.JAXBException;

public class JavaVTLEngine extends VtlParserBaseVisitor<Stream<Statement>> implements Engine, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final StatementFactory statementFactory;
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(JavaVTLEngine.class);
	
	public JavaVTLEngine() throws ClassNotFoundException, JAXBException, IOException 
	{
		statementFactory = new StatementFactory();
	}
	
	public static class ThrowingErrorListener extends BaseErrorListener
	{
		public static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

		@Override
		public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e)
				throws ParseCancellationException
		{
			throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg, e);
		}
	}

	public Statement buildStatement(StatementContext ctx) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException
	{
		return statementFactory.createStatement(ctx);
	}

	@Override
	public Stream<Statement> visitChildren(RuleNode node)
	{
		if (node instanceof StartContext)
		{
			Stream<Statement> result = Stream.empty();
			for (StatementContext ctx: ((StartContext)node).getRuleContexts(StatementContext.class))
				result = Stream.concat(result, ctx.accept(this));
			return result;
		}
		else if (node instanceof StatementContext)
			try
			{
				return Stream.of(buildStatement((StatementContext) node));
			}
			catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException 
					| InvocationTargetException | NoSuchMethodException | ClassNotFoundException | InstantiationException cause)
			{
				throw new RuntimeException(cause);
			}
		else
			return null;
	}

	@Override
	public Stream<Statement> parseRules(String statements)
	{
		VtlParser parser = new VtlParser(new CommonTokenStream(new VtlLexer(CharStreams.fromString(statements))));
		parser.removeErrorListeners();
		parser.addErrorListener(ThrowingErrorListener.INSTANCE);
		parser.getInterpreter().setPredictionMode(LL_EXACT_AMBIG_DETECTION);
		return parser.start().accept(this);
	}
}
