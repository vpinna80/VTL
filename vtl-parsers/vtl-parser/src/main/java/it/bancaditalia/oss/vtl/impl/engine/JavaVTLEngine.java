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

import static it.bancaditalia.oss.vtl.util.SerFunction.identity;
import static java.lang.Thread.currentThread;
import static org.antlr.v4.runtime.atn.PredictionMode.LL_EXACT_AMBIG_DETECTION;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Stream;

import javax.xml.transform.stream.StreamSource;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.RuleNode;
import org.sdmx.vtl.Vtl;
import org.sdmx.vtl.VtlTokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.impl.engine.mapping.OpsFactory;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.ObjectFactory;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Parserconfig;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;

public class JavaVTLEngine extends AbstractParseTreeVisitor<Stream<Statement>> implements Engine, Serializable
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(JavaVTLEngine.class);
	private static final String MAPPING_FILENAME = OpsFactory.class.getName().replaceAll("\\.", "/") + ".xml";
	
	private final OpsFactory opsFactory;
	private final Class<? extends Parser> parserClass;
	private final Class<? extends Lexer> lexerClass;
	
	public JavaVTLEngine() throws ClassNotFoundException, JAXBException, IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException 
	{
		JAXBContext jc = JAXBContext.newInstance(ObjectFactory.class);
		Enumeration<URL> files = Thread.currentThread().getContextClassLoader().getResources(MAPPING_FILENAME);
		if (!files.hasMoreElements())
		{
			IllegalStateException ex = new IllegalStateException("Cannot find VTL mapping file, " + MAPPING_FILENAME);
			LOGGER.error("Cannot find any mapping files. Forgot to add an implementation to your classpath?");
			throw ex;
		}

		boolean first = true;
		URL file = null;
		while (files.hasMoreElements())
			if (first)
			{
				file = files.nextElement();
				first = false;
				LOGGER.info("Using VTL configuration file: {}", file);
			}
			else
			{
				files.nextElement();
				LOGGER.warn("Ignored additional VTL configuration file: {}", file);
				if (!files.hasMoreElements())
					LOGGER.warn("Multiple configurations detected: you may have multiple implementations in your classpath!");
			}
		
		if (file == null)
			throw new FileNotFoundException("VTL mapping configuration file not found in classpath.");

		StreamSource xmlConfig = new StreamSource(file.openStream());
		Parserconfig config = jc.createUnmarshaller().unmarshal(xmlConfig, Parserconfig.class).getValue();
		
		parserClass = Class.forName(config.getParserclass(), true, currentThread().getContextClassLoader()).asSubclass(Parser.class);
		lexerClass = Class.forName(config.getLexerclass(), true, currentThread().getContextClassLoader()).asSubclass(Lexer.class);

		opsFactory = new OpsFactory(config, parserClass, lexerClass);
	}
	
	public Statement buildStatement(ParserRuleContext ctx) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException
	{
		return opsFactory.createStatement(ctx);
	}

	@Override
	public Stream<Statement> visitChildren(RuleNode node)
	{
		if (node instanceof ParserRuleContext && node.getParent() == null)
		{
			List<Stream<Statement>> result = new ArrayList<>();
			for (ParserRuleContext ctx: ((ParserRuleContext) node).getRuleContexts(ParserRuleContext.class))
				result.add(ctx.accept(this));
			
			return result.stream().flatMap(identity());
		}
		else 
			try
			{
				return Stream.of(buildStatement((ParserRuleContext) node));
			}
			catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException 
					| InvocationTargetException | NoSuchMethodException | ClassNotFoundException | InstantiationException cause)
			{
				throw new RuntimeException(cause);
			}
	}

	@Override
	public Stream<Statement> parseRules(String statements)
	{
		Vtl parser = new Vtl(new CommonTokenStream(new VtlTokens(CharStreams.fromString(statements))));
		parser.removeErrorListeners();
		parser.addErrorListener(ThrowingErrorListener.INSTANCE);
		parser.getInterpreter().setPredictionMode(LL_EXACT_AMBIG_DETECTION);
		return parser.start().accept(this);
	}
}
