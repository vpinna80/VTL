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
package it.bancaditalia.oss.vtl.impl.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.xml.bind.JAXBException;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.RuleNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.grammar.Vtl;
import it.bancaditalia.oss.vtl.grammar.Vtl.StartContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.StatementContext;
import it.bancaditalia.oss.vtl.grammar.VtlBaseVisitor;
import it.bancaditalia.oss.vtl.grammar.VtlTokens;
import it.bancaditalia.oss.vtl.impl.engine.statement.StatementFactory;

public class JavaVTLEngine extends VtlBaseVisitor<Stream<Statement>> implements Engine, Serializable
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
			try {
				return Stream.of(buildStatement((StatementContext) node));
			} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException | InstantiationException cause) {
				throw new RuntimeException(cause);
			}
		else
			return null;
	}
	
	private <T> Stream<Statement> addStatementsHelper(T statements, Function<? super T, ? extends CharStream> mapper) 
	{
		return parse(mapper.apply(statements));
	}

	private <T, U> Stream<Statement> addStatementsHelper(T statements, U param,
			BiFunction<? super T, ? super U, ? extends CharStream> mapper)
	{
		return parse(mapper.apply(statements, param));
	}

	private Stream<Statement> parse(CharStream charStream)
	{
		Vtl parser = new Vtl(new CommonTokenStream(new VtlTokens(charStream)));
		parser.removeErrorListeners();
		parser.addErrorListener(ThrowingErrorListener.INSTANCE);
		return parser.start().accept(this);
	}

	@Override
	public Stream<Statement> parseRules(String statements)
	{
		return addStatementsHelper(statements, CharStreams::fromString);
	}

	@Override
	public Stream<Statement> parseRules(Reader reader) throws IOException
	{
		return addStatementsHelper(reader, arg0 -> {
			try
			{
				return CharStreams.fromReader(arg0);
			}
			catch (IOException e)
			{
				throw new UncheckedIOException(e);
			}
		});
	}

	@Override
	public Stream<Statement> parseRules(InputStream inputStream, Charset charset) throws IOException
	{
		return addStatementsHelper(inputStream, charset, (arg0, arg1) -> {
			try
			{
				return CharStreams.fromStream(arg0, arg1);
			}
			catch (IOException e)
			{
				throw new UncheckedIOException(e);
			}
		});
	}

	@Override
	public Stream<Statement> parseRules(Path path, Charset charset) throws IOException
	{
		return addStatementsHelper(path, charset, (t, u) -> {
			try
			{
				return CharStreams.fromPath(t, u);
			}
			catch (IOException e)
			{
				throw new UncheckedIOException(e);
			}
		});
	}
}
