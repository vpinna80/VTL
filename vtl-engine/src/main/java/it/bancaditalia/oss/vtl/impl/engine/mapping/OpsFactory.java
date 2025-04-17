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
package it.bancaditalia.oss.vtl.impl.engine.mapping;

import static it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Type.GROUPBY;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import javax.xml.transform.stream.StreamSource;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.sdmx.vtl.VtlLexer;
import org.sdmx.vtl.VtlParser;
import org.sdmx.vtl.VtlParser.BooleanLiteralContext;
import org.sdmx.vtl.VtlParser.IntegerLiteralContext;
import org.sdmx.vtl.VtlParser.LimitClauseItemContext;
import org.sdmx.vtl.VtlParser.NullLiteralContext;
import org.sdmx.vtl.VtlParser.NumberLiteralContext;
import org.sdmx.vtl.VtlParser.SignedIntegerContext;
import org.sdmx.vtl.VtlParser.StringLiteralContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedContextException;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedTokenException;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Aliasparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Check;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Context;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Contextcheck;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Customparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Customparam.Case;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Exprparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Listparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Mapparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Mapping;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Nestedparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Nonnullparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Nullparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.ObjectFactory;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Param;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Parserconfig;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Roleparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Stringparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenmapping;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenscheck;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenset;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokensetparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Valueparam;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.names.MembershipAlias;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.GroupingClause;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;

public class OpsFactory implements Serializable
{
	private static final long serialVersionUID = 1L;

	private static class VTLParsingException extends RuntimeException
	{
		private static final long serialVersionUID = 1L;

		public VTLParsingException(ParserRuleContext ctx, Throwable t)
		{
			super("In context " + ctx.getClass().getSimpleName() + " on expression: "
					+ ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex())), t);
		}
	}
	
	private static interface ParamBuilder<P extends Nonnullparam> extends Serializable
	{
		public Object apply(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, P param);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(OpsFactory.class);
	private static final String MAPPING_FILENAME = OpsFactory.class.getName().replaceAll("\\.", "/") + ".xml";

	private final Map<Class<? extends Nonnullparam>, ParamBuilder<?>> paramMappers = new HashMap<>();
	private final Map<Class<? extends ParserRuleContext>, List<Mapping>> mappings = new HashMap<>();
	private final Map<String, Tokenset> tokensets = new HashMap<>();
	private final Set<Class<? extends ParserRuleContext>> recursivecontexts = new HashSet<>();
	private final String packageName;

	public OpsFactory() throws JAXBException, ClassNotFoundException, IOException
	{
		paramMappers.put(Tokensetparam.class, (ParamBuilder<Tokensetparam>) this::parseTokensetParam);
		paramMappers.put(Valueparam.class, (ParamBuilder<Valueparam>) this::parseValueParam);
		paramMappers.put(Roleparam.class, (ParamBuilder<Roleparam>) this::parseRoleParam);
		paramMappers.put(Stringparam.class, (ParamBuilder<Stringparam>) this::parseStringParam);
		paramMappers.put(Aliasparam.class, (ParamBuilder<Aliasparam>) this::parseAliasParam);
		paramMappers.put(Listparam.class, (ParamBuilder<Listparam>) this::parseListParam);
		paramMappers.put(Mapparam.class, (ParamBuilder<Mapparam>) this::parseMapParam);
		paramMappers.put(Nestedparam.class, (ParamBuilder<Nestedparam>) this::parseNestedParam);
		paramMappers.put(Customparam.class, (ParamBuilder<Customparam>) this::parseCustomParam);
		paramMappers.put(Exprparam.class, (ParamBuilder<Exprparam>) this::parseExprParam);

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
		packageName = config.getPackage();
		LOGGER.debug("Implementation package: {}", packageName);
		
		LOGGER.debug("Loading mappings");
		for (Mapping mapping : config.getMapping())
			for (String ctxFrom: mapping.getFrom())
			{
				String ctxClassName = VtlParser.class.getName() + "$" + ctxFrom + "Context";
				Class<? extends ParserRuleContext> classFrom = Class.forName(ctxClassName).asSubclass(ParserRuleContext.class);
				mappings.putIfAbsent(classFrom, new ArrayList<>());
				mappings.get(classFrom).add(mapping);
				LOGGER.trace("Loaded mapping {} for context '{}'.", ctxFrom, mapping.getTo());
			}

		LOGGER.debug("Loading tokensets");
		for (Tokenset tokenset : config.getTokenset())
		{
			tokensets.put(tokenset.getName(), tokenset);
			LOGGER.trace("Loaded tokenset {} for tokenset '{}'.", tokenset.getClazz(), tokenset.getName());
		}

		LOGGER.debug("Loading recursive context");
		for (Context context : config.getRecursivecontexts().getContext())
			recursivecontexts.add(Class.forName(VtlParser.class.getName() + "$" + context.getName()).asSubclass(ParserRuleContext.class));
	}

	public Transformation buildExpr(ParserRuleContext ctx)
	{
		String ctxText = ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
		LOGGER.debug("Parsing new context {} containing '{}'.", ctx.getClass().getSimpleName(), ctxText);
		return buildExpr(ctx, null, 0);
	}

	private Transformation buildExpr(ParserRuleContext ctx, GroupingClause currentGroupBy, int level)
	{
		String tabs = new String(new char[level]).replace("\0", "    ");
		Class<? extends ParserRuleContext> ctxClass = ctx.getClass();
		if (recursivecontexts.contains(ctxClass))
		{
			LOGGER.trace("|{}>> Resolving recursive context {}", tabs, ctxClass.getSimpleName());
			Transformation result = buildExpr(ctx.getRuleContext(ParserRuleContext.class, 0), currentGroupBy, level + 1);
			LOGGER.trace("|{}<< Recursive context {} yield {}", tabs, ctxClass.getSimpleName(), result.getClass().getSimpleName());
			return result;
		}

		// Find all mappings that map a context that is the same class or a subclass of given context
		List<Mapping> available = mappings.keySet().stream()
				.filter(c -> c.isAssignableFrom(ctxClass))
				.flatMap(c -> mappings.get(c).stream())
				.collect(toList());

		LOGGER.trace("|{}|| Found {} mappings for {}", tabs, available.size(), ctx.getClass().getSimpleName());
		for (Mapping mapping : available)
			try
			{
				boolean found = checkMapping(mapping.getChecks(), ctx);
				Class<?> target = Class.forName(packageName + "." + mapping.getTo());

				if (found)
				{
					String paramsClasses = mapping.getParams().stream()
							.map(Object::getClass)
							.map(Class::getSimpleName)
							.collect(joining(", ", "{", "}"));

					LOGGER.trace("|{}>> Resolving {} for {}", tabs, paramsClasses, target.getSimpleName());
					List<Object> args = new ArrayList<>();
					List<Param> params = mapping.getParams();

					// find any param indicating a grouping clause
					GroupingClause groupBy = null;
					for (Param param: params)
						if (param.getType() == GROUPBY)
						{
							Object groupByParam = createParam(ctx, param, currentGroupBy, level + 1);
							
							groupBy = param instanceof Nestedparam 
									? (GroupingClause) ((Collection<?>) groupByParam).iterator().next()
									: (GroupingClause) groupByParam;
							
							if (groupBy == null)
								groupBy = currentGroupBy;
							else
								currentGroupBy = groupBy;
							
							break;
						}
					
					for (Param param: params)
					{
						// if the grouping clause is null, fill it with the one from a parent param
						// NOTE: this assumes aggregate invocations cannot be nested
						Object oneOrMoreParam;
						if (param.getType() == GROUPBY)
							oneOrMoreParam = param instanceof Nestedparam ? singleton(currentGroupBy) : currentGroupBy;
						else
							oneOrMoreParam = createParam(ctx, param, currentGroupBy, level + 1);
						
						if (param instanceof Nestedparam)
							args.addAll((Collection<?>) oneOrMoreParam);
						else
							args.add(oneOrMoreParam);
					}

					Constructor<?> constructor = findConstructor(target, args, level);
					LOGGER.trace("|{}<< Invoking constructor for {} with {}", tabs, target.getSimpleName(), args);

					return (Transformation) constructor.newInstance(args.toArray());
				}
			}
			catch (Exception e)
			{
				String expression = ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
				throw new VTLNestedException("In expression " + expression + " from context " + ctxClass, e);
			}

		throw new VTLUnmappedContextException(ctx);
	}

	private Constructor<?> findConstructor(Class<?> target, List<Object> args, int level)
	{
		if (!Transformation.class.isAssignableFrom(target))
			throw new ClassCastException(target + " does not implement " + Transformation.class);

		Constructor<?>[] constructors = target.asSubclass(Transformation.class).getConstructors();

		if (constructors.length < 1)
			throw new IllegalStateException("Expected at least one public constructor but found none for " + target.getSimpleName());

		List<Class<?>> argsClasses = args.stream().map(arg -> arg != null ? arg.getClass() : null).collect(toList());

		for (Constructor<?> constr : constructors)
			if (checkConstructor(constr, target, argsClasses, args, level))
				return constr;

		String text = argsClasses.stream().map(c -> c != null ? c.getSimpleName() : null).collect(joining(", ", "[", "]"));
		throw new IllegalStateException("Could not find a suitable public constructor for " + target.getSimpleName() + " with " + text);
	}

	private boolean checkConstructor(Constructor<?> constr, Class<?> target, List<Class<?>> argsClasses, List<Object> args, int level)
	{
		if (constr.getParameterCount() != args.size())
			return false;
		
		Class<?>[] parameterTypes = constr.getParameterTypes();
		
		for (int i = 0; i < args.size(); i++)
			if (argsClasses.get(i) != null && !parameterTypes[i].isAssignableFrom(argsClasses.get(i)))
				return false;

		String argsClsStr = argsClasses.stream()
				.map(c -> c != null ? c.getSimpleName() : "")
				.collect(joining(", ", "{", "}"));

		String tabs = new String(new char[level]).replace("\0", "    ");
		LOGGER.trace("|{}|| Found constructor for {} with {}", tabs, target.getSimpleName(), argsClsStr);
		return true;
	}

	private boolean checkMapping(Check check, ParserRuleContext ctx)
	{
		Class<? extends ParserRuleContext> ctxClass = ctx.getClass();

		try
		{
			boolean checkIsValid = check == null;
			if (!checkIsValid)
				if (check instanceof Tokenscheck)
				{
					Tokenscheck tokens = (Tokenscheck) check;
					for (String value : tokens.getValue())
						if (!checkIsValid)
							checkIsValid = searchToken(ctx, tokens, value);
				}
				else if (check instanceof Contextcheck)
				{
					Contextcheck context = (Contextcheck) check;
					Class<? extends ParserRuleContext> target = Class.forName(VtlParser.class.getName() + "$" + context.getContext())
							.asSubclass(ParserRuleContext.class);
					Class<? extends Object> childruleclass = ctxClass.getField(context.getName()).get(ctx).getClass();
					if (target == childruleclass)
						checkIsValid = true;
				}
				else
					throw new UnsupportedOperationException("Check of class " + check.getClass().getSimpleName() + " not implemented.");

			return checkIsValid;
		}
		catch (ClassNotFoundException | IllegalAccessException | NoSuchFieldException e)
		{
			throw new VTLParsingException(ctx, e);
		}
	}

	private boolean searchToken(ParserRuleContext ctx, Tokenscheck tokens, String value)
	{
		try
		{
			boolean found = false;
			Class<? extends ParserRuleContext> ctxClass = ctx.getClass();
			
			if (tokens.getOrdinal() != null)
			{
				int type = ((Token) ctx.getChild(TerminalNode.class, tokens.getOrdinal() - 1).getPayload()).getType();
				if (type == VtlLexer.class.getField(value).getInt(null))
					return true;
			}
			else
				try
				{
					if (((Token) ctxClass.getField(tokens.getName()).get(ctx)).getType() == VtlLexer.class.getField(value).getInt(null))
					{
						LOGGER.trace("Found token {}.", value);
						found = true;
					}
				}
				catch (NoSuchFieldException e)
				{
					ParseTree rule = (ParseTree) ctxClass.getMethod(tokens.getName()).invoke(ctx);
					if (rule != null)
						try
						{
							TerminalNode leaf = (TerminalNode) rule.getClass().getMethod(value).invoke(rule);
							if (leaf != null && ((Token) leaf.getPayload()).getType() == VtlLexer.class.getField(value).getInt(null))
							{
								LOGGER.trace("Found token {}.", value);
								found = true;
							}
						}
						catch (NoSuchMethodException e1)
						{
							return false;
						}
				}
			
			return found;
		}
		catch (NoSuchMethodException | NoSuchFieldException | IllegalAccessException | InvocationTargetException e)
		{
			throw new VTLParsingException(ctx, e);
		}
	}

	private <P extends Nonnullparam> Object createParam(ParserRuleContext ctx, Param maybeNullParam, GroupingClause currentGroupBy, int level)
	{
		String tabs = new String(new char[level]).replace("\0", "    ");
		
		String ctxClass = requireNonNull(ctx, "Parsing context is null").getClass().getSimpleName();
		
		if (recursivecontexts.contains(ctx.getClass()))
		{
			LOGGER.trace("|{}++ {}: recursive context", tabs, ctxClass);
			return createParam(ctx.getChild(ParserRuleContext.class, 0), maybeNullParam, currentGroupBy, level);
		}
		
		if (maybeNullParam instanceof Nullparam)
		{
			LOGGER.trace("|{}>> {}: Null", tabs, ctxClass);
			LOGGER.trace("|{}<< {}: Null", tabs, ctxClass);
			return null;
		}
		else
		{
			@SuppressWarnings("unchecked")
			P param = (P) maybeNullParam;
			
			Class<? extends Nonnullparam> paramClass = param.getClass();
			String paramClassName = paramClass.getSimpleName();
			Object result;
			if (param.getName() != null)
				LOGGER.trace("|{}>> {}: {} from subrule '{}'", tabs, ctxClass, paramClassName, param.getName());
			else
				LOGGER.trace("|{}>> {}: {} from same context", tabs, ctxClass, paramClassName);

			@SuppressWarnings("unchecked")
			ParamBuilder<P> contextParser = (ParamBuilder<P>) paramMappers.get(paramClass);
			if (contextParser == null)
				throw new IllegalStateException("Not implemented: " + paramClassName);
				
			result = contextParser.apply(ctx, currentGroupBy, level, (P) param);

			if (param.getName() != null)
				LOGGER.trace("|{}<< {}: {} from subrule '{}' yield {}", tabs, ctxClass, paramClassName, param.getName(), result);
			else
				LOGGER.trace("|{}<< {}: {} from same context yield {}", tabs, ctxClass, paramClassName, result);

			return result;
		}
	}

	private Transformation parseExprParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Exprparam param)
	{
		Transformation result;
		ParserRuleContext subexpr = getFieldOrMethod(param, ctx, ParserRuleContext.class, level);
		result = subexpr == null ? null : buildExpr(subexpr, currentGroupBy, level + 1);
		return result;
	}

	private Object parseCustomParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Customparam customParam)
	{
		ParserRuleContext customCtx = null;
		List<Param> innerParams = null;
		List<Object> resultList = null;
		Class<?> customClass = null;
		try
		{
			// get the nested context by looking up the name attribute of nestedparam in current context
			customCtx = getFieldOrMethod(customParam, ctx, ParserRuleContext.class, level);
			
			if (customCtx == null)
				return null;
			
			List<Serializable> customSpec = customParam.getCaseOrNullparamOrAliasparam();
			if (!customSpec.isEmpty() && customSpec.get(0).getClass() == Case.class)
			{
				Case found = null;
				@SuppressWarnings("unchecked")
				List<Case> cases = (List<Case>) (List<?>) customSpec;
				for (Case c: cases)
					if (found == null && checkMapping(c.getChecks(), customCtx))
						found = c;
				
				if (found != null)
					innerParams = found.getParams();
				else
					throw new IllegalStateException("No matching cases for customparam");
			}
			else
				innerParams = customSpec.stream().map(Param.class::cast).collect(toList());
			
			resultList = new ArrayList<>(innerParams.size());
			for (Param child : innerParams)
				resultList.add(createParam(customCtx, child, currentGroupBy, level + 1));
			
			customClass = Class.forName(customParam.getClazz());
			if (customParam.getMethod() != null)
				return Arrays.stream(customClass.getMethods())
						.filter(m -> m.getName().equals(customParam.getMethod()))
						.findAny()
						.orElseThrow(() -> new NoSuchMethodException(customParam.getMethod()))
						.invoke(null, resultList.toArray());
			else
				for (Constructor<?> ctor: customClass.getConstructors())
				{					
					Class<?>[] types = ctor.getParameterTypes();
					boolean bad = false;
					for (int i = 0; !bad && i < ctor.getParameterCount(); i++)
					{
						Object ith = resultList.get(i);
						if (ith != null && !types[i].isAssignableFrom(ith.getClass()))
							bad = true;
					}
					
					if (!bad)
						return ctor.newInstance(resultList.toArray());
				}
			
			throw new NoSuchMethodException(customParam.getMethod());
		}
		catch (Exception e)
		{
			throw new VTLParsingException(ctx, e);
		}
	}

	private Object parseNestedParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Nestedparam nestedParam)
	{
		Object result;
		// get the nested context by looking up the name attribute of nestedparam in current context
		ParserRuleContext nestedCtx = getFieldOrMethod(nestedParam, ctx, ParserRuleContext.class, level);
		// iteratively resolve any parameters inside the nested context 
		List<Param> innerParams = nestedParam.getParams();
		// map each parameter to a constructed mapped object and collect the results into a list
		List<Object> resultList = new ArrayList<>(innerParams.size());
		for (Param child : innerParams)
			resultList.add(nestedCtx == null ? null : createParam(nestedCtx, child, currentGroupBy, level + 1));
		result = resultList;
		return result;
	}

	private Map<?, ?> parseMapParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Mapparam mapparam)
	{
		Map<?, ?> result;
		Map<Object, Object> resultMap = new HashMap<>();
		@SuppressWarnings("unchecked")
		List<? extends ParserRuleContext> entries = getFieldOrMethod(mapparam, ctx, List.class, level);
		Param keyParam = mapparam.getKey().getParams();
		Param valueParam = mapparam.getValue().getParams();
		for (ParserRuleContext entry : entries)
		{
			Object key = createParam(entry, keyParam, currentGroupBy, level + 1);
			Object value = createParam(entry, valueParam, currentGroupBy, level + 1);
			resultMap.put(key, value);
		}
		result = resultMap;
		return result;
	}

	private List<?> parseListParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Listparam listParam)
	{
		List<?> result;
		Param insideParam = listParam.getParams();
		@SuppressWarnings("unchecked")
		Collection<? extends ParserRuleContext> inside = getFieldOrMethod(listParam, ctx, Collection.class, level);
		List<Object> resultList = new ArrayList<>();
		for (ParserRuleContext child : inside)
			resultList.add(createParam(child, insideParam, currentGroupBy, level + 1));
		result = resultList;
		return result;
	}

	private String parseStringParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Stringparam stringparam)
	{
		String result;
		Object value = getFieldOrMethod(stringparam, ctx, Object.class, level);
		if (value instanceof Token)
			result = ((Token) value).getText();
		else if (value instanceof ParseTree)
			result = ((ParseTree) value).getText();
		else
			result = null;
		return result;
	}

	private VTLAlias parseAliasParam(ParserRuleContext ctx, Object currentGroupBy, int level, Aliasparam stringparam)
	{
		String result;
		Object value = getFieldOrMethod(stringparam, ctx, Object.class, level);
		if (value instanceof Token)
			result = ((Token) value).getText();
		else if (value instanceof ParseTree)
			result = ((ParseTree) value).getText();
		else
			result = null;
		
		// TODO: check for single quotes 
		if (result != null && result.contains("#"))
		{
			String[] split = result.split("#");
			return new MembershipAlias(VTLAliasImpl.of(split[0]), VTLAliasImpl.of(split[1]));
		}
		else
			return VTLAliasImpl.of(result);
	}

	private Class<? extends Component> parseRoleParam(ParserRuleContext ctx, Object currentGroupBy, int level, Roleparam param)
	{
		// lookup actual token
		ParseTree roleCtx = getFieldOrMethod(param, ctx, ParseTree.class, level);
		Deque<ParseTree> stack = new LinkedList<>();
		List<Token> resultList = new ArrayList<>();
		stack.push(roleCtx);
		while (!stack.isEmpty())
		{
			ParseTree current = stack.pop();
			if (current instanceof TerminalNode)
				resultList.add((Token) current.getPayload());
			else if (current instanceof RuleContext)
				IntStream.range(0, current.getChildCount())
					.forEachOrdered(i -> stack.push(current.getChild(i)));
			else if (current != null)
				throw new IllegalStateException("Unexpected ParseTree of " + current.getClass());
		}

		Optional<Token> firstToken = Optional.ofNullable(resultList.isEmpty() ? null : resultList.get(0));
		Optional<Token> secondToken = Optional.ofNullable(resultList.size() < 2 ? null : resultList.get(1));
		
		if (!firstToken.isPresent())
			return null;
		else if (!secondToken.isPresent())
			switch (firstToken.get().getType())
			{
				case VtlParser.MEASURE: return Measure.class;
				case VtlParser.DIMENSION: return Identifier.class;
				case VtlParser.ATTRIBUTE: return Attribute.class;
				default: 
					throw new IllegalStateException("Unrecognized role token " + VtlParser.VOCABULARY.getSymbolicName(firstToken.get().getType()) 
							+ " containing " + firstToken.get().getText());
			}
		else if (firstToken.get().getType() == VtlParser.VIRAL && secondToken.get().getType() == VtlParser.ATTRIBUTE)
			return ViralAttribute.class;
		else
		{
			throw new IllegalStateException("Unrecognized role token " + VtlParser.VOCABULARY.getSymbolicName(firstToken.get().getType()) 
					+ " containing " + firstToken.get().getText());
		}
	}

	private ScalarValue<?, ?, ?, ?> parseValueParam(ParserRuleContext ctx, Object currentGroupBy, int level, Valueparam param)
	{
		// lookup actual token
		ParseTree element = getFieldOrMethod(param, ctx, ParseTree.class, level);
		if (element == null)
			return null;
		
		if (element instanceof RuleNode)
			element = resolveRecursiveContext((ParserRuleContext) element);
		
		String text = element.getText();
		if (element instanceof StringLiteralContext)
			return StringValue.of(text.matches("^\".*\"$") ? text.substring(1, text.length() - 1) : text);
		if (element instanceof IntegerLiteralContext || element instanceof SignedIntegerContext)
			return IntegerValue.of(Long.parseLong(text));
		if (element instanceof NumberLiteralContext)
			return NumberValueImpl.createNumberValue(text);
		if (element instanceof NullLiteralContext)
			return NullValue.instance(NULLDS);
		if (element instanceof BooleanLiteralContext)
			return BooleanValue.of(Boolean.parseBoolean(text));
		if (element instanceof LimitClauseItemContext)
		{
			if (((LimitClauseItemContext) element).UNBOUNDED() != null) 
				return IntegerValue.of((long) (Integer.MAX_VALUE));
			if (((LimitClauseItemContext) element).CURRENT() != null) 
				return IntegerValue.of(0L);

			return IntegerValue.of(Long.parseLong(element.getChild(0).getText()));
		}
		
		if (element instanceof TerminalNode)
		{
			Token token = ((TerminalNode) element).getSymbol();
			switch (token.getType())
			{
				case VtlParser.STRING_CONSTANT: return StringValue.of(text.matches("^\".*\"$") ? text.substring(1, text.length() - 1) : text);
				default: throw new VTLUnmappedTokenException(text, param);
			}
		}
		else
			throw new VTLUnmappedContextException((ParserRuleContext) element);
	}

	private ParserRuleContext resolveRecursiveContext(ParserRuleContext ctx)
	{
		while (recursivecontexts.contains(ctx.getClass()) && !isNull(ctx.getChild(ParserRuleContext.class, 0)))
			ctx = ctx.getChild(ParserRuleContext.class, 0);
		
		return ctx;
	}

	private Enum<?> parseTokensetParam(ParserRuleContext ctx, Object currentGroupBy, int level, Tokensetparam tokensetParam)
	{
		try
		{
			Enum<?> result;
			// get the tokenset
			Tokenset tokenset = requireNonNull(tokensets.get(tokensetParam.getTokenset()),
					"Tokenset " + tokensetParam.getTokenset() + " not found in mapping");
			// lookup actual token
			Object rule = getFieldOrMethod(tokensetParam, ctx, Object.class, level);
			if (rule == null)
				result = null;
			else
			{
				if (rule instanceof ParserRuleContext)
					rule = ((ParserRuleContext) rule).getChild(TerminalNode.class, 0);
				if (rule instanceof TerminalNode)
					rule = ((TerminalNode) rule).getPayload();
				if (rule instanceof Token)
				{
					String ruleText = ((Token) rule).getText();
					String sourceToken = VtlLexer.VOCABULARY.getSymbolicName(((Token) rule).getType());
					// find corresponding enum value
					Set<Tokenmapping> matchingTokens = tokenset.getTokenmapping().stream()
							.filter(t -> t.getName().equals(sourceToken) || t.getName().equalsIgnoreCase(ruleText))
							.collect(toSet());
					if (matchingTokens.size() != 1)
						throw new VTLUnmappedTokenException(sourceToken, tokenset);
					// get the enum value
					Enum<?> enumValue = Enum.valueOf(Class.forName(tokenset.getClazz()).asSubclass(Enum.class), matchingTokens.iterator().next().getValue());
					result = enumValue;
				}
				else
					result = null;
			}
			return result;
		}
		catch (ClassNotFoundException e)
		{
			throw new VTLParsingException(ctx, e);
		}
	}

	private <T> T getFieldOrMethod(Nonnullparam param, RuleContext ctx, Class<T> resultClass, int level)
	{
		String tabs = "";
		for (int i = 0; i <= level; i++)
			tabs += "    ";
		Class<? extends RuleContext> ctxClass = ctx.getClass();
		
		Object result = ctx;
		if (param.getName() != null)
		{
			List<Exception> suppressed = new ArrayList<>();
			List<String> names = asList(param.getName().split("\\|"));
			boolean found = false;
			for (String name: names)
				try
				{
					LOGGER.trace("|{}>> Looking up subrule '{}' as a {}", tabs, name, param.getClass().getSimpleName());
					result = ctxClass.getField(name).get(ctx);
					found = true;
					break;
				}
				catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e)
				{
					try
					{
						result = ctxClass.getMethod(name).invoke(ctx);
						found = true;
						break;
					}
					catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e1)
					{
						suppressed.add(e);
						suppressed.add(e1);
					}
				}
			
			if (!found)
			{
				IllegalStateException e = new IllegalStateException("No field or method with names in " + names);
				for (Exception e1: suppressed)
					e.addSuppressed(e1);
				throw e;
			}
		}
		else
			LOGGER.trace("|{}>> Looking up context {}", tabs, ctx.getClass().getSimpleName());
		
		if (param.getOrdinal() != null)
			if (result instanceof List)
				result = ((List<?>) result).get(param.getOrdinal().intValue());
			else
				result = ((RuleContext) result).getChild(param.getOrdinal().intValue() - 1);

		if (result instanceof ParserRuleContext)
		{
			ParserRuleContext resultCtx = (ParserRuleContext) result;
			String ctxText = resultCtx.start.getInputStream().getText(new Interval(resultCtx.start.getStartIndex(), resultCtx.stop.getStopIndex()));
			LOGGER.trace("|{}<< Found child context {} with value '{}'", tabs, result.getClass().getSimpleName(), ctxText);
		}
		else if (result instanceof Token)
		{
			Token token = (Token) result;
			String sourceToken = VtlLexer.VOCABULARY.getSymbolicName(token.getType());
			LOGGER.trace("|{}<< Found token {} with value '{}'", tabs, sourceToken, token.getText());
		}
		else if (result instanceof TerminalNode)
		{
			Token token = (Token) ((TerminalNode) result).getPayload();
			String sourceToken = VtlLexer.VOCABULARY.getSymbolicName(token.getType());
			LOGGER.trace("|{}<< Found token {} with value '{}'", tabs, sourceToken, token.getText());
		}
		else if (result != null)
			LOGGER.trace("|{}<< Found result {} with value {}", tabs, result.getClass(), result);
		else
			LOGGER.trace("|{}<< Found null result", tabs);

		return resultClass.cast(result);
	}
}
