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
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.sdmx.vtl.Vtl.LimitClauseItemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedContextException;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedTokenException;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Aliasparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Check;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Componentparamtype;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Context;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Contextcheck;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Customparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Customparam.Case;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Datasetparamtype;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Exprparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Listparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Mapparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Mapping;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Nestedparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Nonnullparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Nullparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Param;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Paramparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Parserconfig;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Roleparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Roletokens;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Scalarparamtype;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Statementdef;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Stringparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenmapping;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenscheck;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokenset;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Tokensetparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Typeparam;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Valuecontexts;
import it.bancaditalia.oss.vtl.impl.engine.mapping.xml.Valueparam;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.names.MembershipAlias;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.ComponentParameterTypeImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.DataSetParameterTypeImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.ParameterImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.QuantifiedComponent;
import it.bancaditalia.oss.vtl.impl.types.statement.QuantifiedComponent.Multiplicity;
import it.bancaditalia.oss.vtl.impl.types.statement.ScalarParameterTypeImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Role;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.ComponentParameterType;
import it.bancaditalia.oss.vtl.model.transform.GroupingClause;
import it.bancaditalia.oss.vtl.model.transform.Parameter;
import it.bancaditalia.oss.vtl.model.transform.ParameterType;
import it.bancaditalia.oss.vtl.model.transform.ScalarParameterType;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.SerFunction;
import jakarta.xml.bind.JAXBException;

public class OpsFactory implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(OpsFactory.class);

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

	private final Map<Class<? extends Nonnullparam>, ParamBuilder<?>> paramMappers = new HashMap<>();
	private final Map<Class<? extends ParserRuleContext>, List<Mapping>> mappings = new HashMap<>();
	private final Map<Class<? extends ParserRuleContext>, List<Statementdef>> statements = new HashMap<>();
	private final Map<String, Tokenset> tokensets = new HashMap<>();
	private final Set<Class<? extends ParserRuleContext>> recursivecontexts = new HashSet<>();
	private final String packageName;
	private final Class<?> parserClass;
	private final Map<Integer, Role> roleTokens = new HashMap<>();
	private final Vocabulary vocabulary;
	private final Map<String, Integer> tokenTypes = new HashMap<>();
	private final Map<Class<?>, SerFunction<String, ? extends ScalarValue<?, ?, ?, ?>>> valueBuilders = new HashMap<>();
	private final Class<? extends ParserRuleContext> scalarParamClass;
	private final Class<? extends ParserRuleContext> datasetParamClass;
	private final Class<? extends ParserRuleContext> componentParamClass;
	private final Scalarparamtype scalarParam;
	private final Datasetparamtype datasetParam;
	private final Componentparamtype componentParam;

	public OpsFactory(Parserconfig config, Class<? extends Parser> parserClass, Class<? extends Lexer> lexerClass) throws JAXBException, ClassNotFoundException, IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
	{
		this.parserClass = parserClass;
		
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
		paramMappers.put(Paramparam.class, (ParamBuilder<Paramparam>) this::parseParamParam);
		paramMappers.put(Typeparam.class, (ParamBuilder<Typeparam>) this::parseTypeParam);

		packageName = config.getPackage();
		LOGGER.debug("Implementation package: {}", packageName);
		
		vocabulary = Vocabulary.class.cast(parserClass.getField("VOCABULARY").get(null));
		for (int i = 0; i <= vocabulary.getMaxTokenType(); i++)
			tokenTypes.put(vocabulary.getSymbolicName(i), i);
		Valuecontexts valueContexts = config.getValuecontexts();
		Map<List<String>, SerFunction<String, ScalarValue<?, ?, ?, ?>>> builders = Map.of(
		    valueContexts.getInteger(), text -> IntegerValue.of(Long.parseLong(text)),
		    valueContexts.getString(), text -> StringValue.of(text.matches("^\".*\"$") ? text.substring(1, text.length() - 1) : text),
		    valueContexts.getNumber(), NumberValueImpl::createNumberValue,
		    valueContexts.getBoolean(), text -> BooleanValue.of(Boolean.parseBoolean(text)),
		    valueContexts.getNull(), text -> NullValue.instance(NULLDS)
		);
		for (Entry<List<String>, SerFunction<String, ScalarValue<?, ?, ?, ?>>> entry : builders.entrySet())
		    for (String rules: entry.getKey())
		    	for (String ctx: rules.split(","))
		    		try
					{
		    			valueBuilders.put(getContextClass(ctx), entry.getValue());
					}
					catch (NoClassDefFoundError | ClassNotFoundException e)
					{
						throw new VTLNestedException("Cannot load required class", e);
					}
		LOGGER.debug("Parser class: {}", parserClass.getName());
		
		Roletokens roletokens = config.getRoletokens();
		roleTokens.put(parserClass.getField(roletokens.getComponent()).getInt(null), Role.COMPONENT);
		roleTokens.put(parserClass.getField(roletokens.getIdentifier()).getInt(null), Role.IDENTIFIER);
		roleTokens.put(parserClass.getField(roletokens.getMeasure()).getInt(null), Role.MEASURE);
		roleTokens.put(parserClass.getField(roletokens.getAttribute()).getInt(null), Role.ATTRIBUTE);
		roleTokens.put(parserClass.getField(roletokens.getViralattribute()).getInt(null), Role.VIRAL_ATTRIBUTE);
		LOGGER.debug("Component role tokens: {} {} {} {}", roletokens.getIdentifier(), roletokens.getMeasure(), roletokens.getAttribute(), roletokens.getViralattribute());

		scalarParamClass = getContextClass(config.getParametertypes().getScalar().getFrom());
		scalarParam = config.getParametertypes().getScalar();
		componentParamClass = getContextClass(config.getParametertypes().getComponent().getFrom());
		componentParam = config.getParametertypes().getComponent();
		datasetParamClass = getContextClass(config.getParametertypes().getDataset().getFrom());
		datasetParam = config.getParametertypes().getDataset();
		
		LOGGER.debug("Loading tokensets");
		for (Tokenset tokenset : config.getTokenset())
		{
			tokensets.put(tokenset.getName(), tokenset);
			LOGGER.trace("Loaded tokenset {} for tokenset '{}'.", tokenset.getClazz(), tokenset.getName());
		}

		LOGGER.debug("Loading recursive context");
		for (Context context : config.getRecursivecontexts().getContext())
			recursivecontexts.add(Class.forName(parserClass.getName() + "$" + context.getName()).asSubclass(ParserRuleContext.class));

		LOGGER.debug("Loading mappings");
		for (Mapping mapping : config.getMapping())
			for (String ctxFrom: mapping.getFrom())
			{
				Class<? extends ParserRuleContext> classFrom = getContextClass(ctxFrom);
				mappings.putIfAbsent(classFrom, new ArrayList<>());
				mappings.get(classFrom).add(mapping);
				LOGGER.trace("Loaded mapping {} for context '{}'.", ctxFrom, mapping.getTo());
			}

		LOGGER.debug("Loading statements");
		for (Statementdef statement : config.getStatementdef())
			for (String ctxFrom: statement.getFrom())
			{
				Class<? extends ParserRuleContext> classFrom = getContextClass(ctxFrom);
				statements.putIfAbsent(classFrom, new ArrayList<>());
				statements.get(classFrom).add(statement);
				LOGGER.trace("Loaded mapping {} for context '{}'.", ctxFrom, statement.getTo());
			}
	}

	// Creates a top-level statement, such as assignment or define ruleset
	public Statement createStatement(ParserRuleContext ctx) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException
	{
		String ctxText = ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
		LOGGER.debug("Parsing new context {} containing '{}'.", ctx.getClass().getSimpleName(), ctxText);
		
		ParserRuleContext statementCtx = resolveRecursiveContext(ctx, 0);
		Class<? extends ParserRuleContext> ctxClass = statementCtx.getClass();
		
		// Find all mappings that map a context that is the same class or a subclass of given context
		List<Statementdef> available = statements.keySet().stream()
				.filter(c -> c.isAssignableFrom(ctxClass))
				.flatMap(c -> statements.get(c).stream())
				.collect(toList());

		LOGGER.trace("||| Found {} mappings for {}", available.size(), statementCtx.getClass().getSimpleName());
		for (Statementdef statement : available)
			try
			{
				boolean found = checkMapping(statement.getChecks(), statementCtx);
				Class<?> target = Class.forName(packageName + "." + statement.getTo());

				if (found)
				{
					String paramsClasses = statement.getParams().stream()
							.map(Object::getClass)
							.map(Class::getSimpleName)
							.collect(joining(", ", "{", "}"));

					LOGGER.trace("|>> Resolving {} for {}", paramsClasses, target.getSimpleName());
					List<Object> args = new ArrayList<>();
					List<Param> params = statement.getParams();

					for (Param param: params)
					{
						Object arg = parseGenericParam(statementCtx, null, 1, param);
						if (param instanceof Nestedparam)
							args.addAll((Collection<?>) arg);
						else
							args.add(arg);
					}

					Constructor<Statement> constructor = findConstructor(Statement.class, target, args, 0);
					LOGGER.trace("|<< Invoking constructor for {} with {}", target.getSimpleName(), args);

					return constructor.newInstance(args.toArray());
				}
			}
			catch (Exception e)
			{
				String expression = statementCtx.start.getInputStream().getText(new Interval(statementCtx.start.getStartIndex(), statementCtx.stop.getStopIndex()));
				throw new VTLNestedException("In expression " + expression + " from context " + ctxClass, e);
			}

		throw new VTLUnmappedContextException(statementCtx);

//		if (ctx instanceof DefineExpressionContext)
//		{
//			DefOperatorsContext defineContext = ((DefineExpressionContext) ctx).defOperators();
//			if (defineContext instanceof DefOperatorContext)
//			{
//				DefOperatorContext defineOp = (DefOperatorContext) defineContext;
//				List<Parameter> params = coalesce(defineOp.parameterItem(), emptyList()).stream()
//						.map(this::buildParam)
//						.collect(toList());
//				OutputParameterTypeContext type = defineOp.outputParameterType();
//				BaseParameter outputType = type == null ? null : buildType(type, null, type.scalarType(), type.componentType(), type.datasetType());
//				
//				return new DefineOperatorStatement(VTLAliasImpl.of(defineOp.operatorID().getText()), params, outputType, buildExpr(defineOp.expr()));
//			}
//			else if (defineContext instanceof DefHierarchicalContext)
//			{
//				DefHierarchicalContext defineHier = (DefHierarchicalContext) defineContext;
//				VTLAlias alias = VTLAliasImpl.of(defineHier.rulesetID().getText());
//				RuleSetType rulesetType = defineHier.hierRuleSignature().VALUE_DOMAIN() != null ? VALUE_DOMAIN : VARIABLE;
//				VTLAlias ruleID = defineHier.hierRuleSignature().IDENTIFIER() != null ? VTLAliasImpl.of(defineHier.hierRuleSignature().IDENTIFIER().getText()) : null;
//				
//				List<VTLAlias> names = new ArrayList<>();
//				List<String> leftOps = new ArrayList<>();
//				List<Transformation> whenOps = new ArrayList<>();
//				List<RuleType> compOps = new ArrayList<>();
//				List<Map<String, Boolean>> rightOps = new ArrayList<>();
//				List<String> ercodes = new ArrayList<>();
//				List<Long> erlevels = new ArrayList<>();
//				
//				for (int i = 0; defineHier.ruleClauseHierarchical().ruleItemHierarchical(i) != null; i++)
//				{
//					RuleItemHierarchicalContext rule = defineHier.ruleClauseHierarchical().ruleItemHierarchical(i);
//					names.add(rule.ruleName != null ? VTLAliasImpl.of(rule.ruleName.getText()) : null);
//					String erCode = null;
//					if (rule.erCode() != null)
//					{
//						erCode = rule.erCode().constant().getText();
//						erCode = erCode.substring(1, erCode.length() - 1);
//					}
//					ercodes.add(erCode);
//					erlevels.add(rule.erLevel() != null ? Long.parseLong(rule.erLevel().constant().getText()) : null);
//
//					CodeItemRelationContext relation = rule.codeItemRelation();
//					switch (relation.comparisonOperand().getStart().getType())
//					{
//						case Vtl.EQ: compOps.add(RuleType.EQ); break;
//						case Vtl.LT: compOps.add(RuleType.LT); break;
//						case Vtl.LE: compOps.add(RuleType.LE); break;
//						case Vtl.MT: compOps.add(RuleType.GT); break;
//						case Vtl.ME: compOps.add(RuleType.GE); break;
//						default: throw new UnsupportedOperationException("Invalid operand in ruleset rule " + rule.ruleName.getText() + ": " + relation.comparisonOperand().getText());
//					}
//					
//					ExprComponentContext when = relation.exprComponent();
//					
//					leftOps.add(relation.codetemRef.getText());
//					whenOps.add(when != null ? buildExpr(when): null);
//					rightOps.add(relation.codeItemRelationClause().stream()
//						.collect(toMap(r -> r.rightCodeItem.getText(), r -> r.opAdd == null || r.opAdd.getType() == Vtl.PLUS)));
//				}
//				
//				return new DefineHierarchyStatement(alias, rulesetType, ruleID, names, leftOps, compOps, whenOps, rightOps, ercodes, erlevels);
//			}
//			else if (defineContext instanceof DefDatapointRulesetContext)
//			{
//				DefDatapointRulesetContext defineDatapoint = (DefDatapointRulesetContext) defineContext;
//
//				VTLAlias alias = VTLAliasImpl.of(defineDatapoint.rulesetID().getText());
//				RuleSetType rulesetType = defineDatapoint.rulesetSignature().VALUE_DOMAIN() != null ? VALUE_DOMAIN : VARIABLE;
//				List<Entry<VTLAlias, VTLAlias>> vars = defineDatapoint.rulesetSignature().signature().stream()
//						.map(s -> new SimpleEntry<VTLAlias, VTLAlias>(VTLAliasImpl.of(s.varID().getText()), s.alias() != null ? VTLAliasImpl.of(s.alias().getText()) : null))
//						.collect(toList());
//				
//				List<VTLAlias> ruleIDs = new ArrayList<>();
//				List<Transformation> conds = new ArrayList<>();
//				List<Transformation> exprs = new ArrayList<>();
//				List<String> ercodes = new ArrayList<>();
//				List<Long> erlevels = new ArrayList<>();
//
//				for (RuleItemDatapointContext rule: defineDatapoint.ruleClauseDatapoint().ruleItemDatapoint())
//				{
//					ruleIDs.add(rule.ruleName != null ? VTLAliasImpl.of(rule.ruleName.getText()) : null);
//					conds.add(rule.antecedentContiditon != null ? buildExpr(rule.antecedentContiditon) : null);
//					exprs.add(buildExpr(rule.consequentCondition));
//					ercodes.add(rule.erCode() != null ? rule.erCode().constant().getText() : null);
//					erlevels.add(rule.erLevel() != null ? Long.parseLong(rule.erLevel().constant().getText()) : null);
//				}
//
//				return new DefineDataPointStatement(alias, rulesetType, vars, ruleIDs, conds, exprs, ercodes, erlevels);
//			}
//			else
//				throw new VTLUnmappedContextException(defineContext);
//		}
//		else
//			throw new VTLUnmappedContextException(ctx);
	}

	private Class<? extends ParserRuleContext> getContextClass(String contextName) throws ClassNotFoundException
	{
		String ctxClassName = parserClass.getName() + "$" + contextName + "Context";
		return Class.forName(ctxClassName, true, currentThread().getContextClassLoader()).asSubclass(ParserRuleContext.class);
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
							Object groupByParam = parseGenericParam(ctx, currentGroupBy, level + 1, param);
							
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
							oneOrMoreParam = parseGenericParam(ctx, currentGroupBy, level + 1, param);
						
						if (param instanceof Nestedparam)
							args.addAll((Collection<?>) oneOrMoreParam);
						else
							args.add(oneOrMoreParam);
					}

					Constructor<Transformation> constructor = findConstructor(Transformation.class, target, args, level);
					LOGGER.trace("|{}<< Invoking constructor for {} with {}", tabs, target.getSimpleName(), args);

					return constructor.newInstance(args.toArray());
				}
			}
			catch (Exception e)
			{
				String expression = ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
				throw new VTLNestedException("In expression " + expression + " from context " + ctxClass, e);
			}

		throw new VTLUnmappedContextException(ctx);
	}

	@SuppressWarnings("unchecked")
	private <T> Constructor<T> findConstructor(Class<T> base, Class<?> target, List<Object> args, int level)
	{
		if (!base.isAssignableFrom(target))
			throw new ClassCastException(target + " does not implement " + Transformation.class);

		Constructor<?>[] constructors = target.asSubclass(base).getConstructors();

		if (constructors.length < 1)
			throw new IllegalStateException("Expected at least one public constructor but found none for " + target.getSimpleName());

		List<Class<?>> argsClasses = args.stream().map(arg -> arg != null ? arg.getClass() : null).collect(toList());

		for (Constructor<?> constr : constructors)
			if (checkConstructor(constr, target, argsClasses, args, level))
				return (Constructor<T>) constr;

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
					Class<? extends ParserRuleContext> target = Class.forName(parserClass.getName() + "$" + context.getContext())
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
				if (type == tokenTypes.get(value))
					return true;
			}
			else
				try
				{
					if (((Token) ctxClass.getField(tokens.getName()).get(ctx)).getType() == tokenTypes.get(value))
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
							if (leaf != null && ((Token) leaf.getPayload()).getType() == tokenTypes.get(value))
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
		catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
		{
			throw new VTLParsingException(ctx, e);
		}
	}

	private <P extends Nonnullparam> Object parseGenericParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Param maybeNullParam)
	{
		String tabs = new String(new char[level]).replace("\0", "    ");
		
		String ctxClass = requireNonNull(ctx, "Parsing context is null").getClass().getSimpleName();
		
		if (recursivecontexts.contains(ctx.getClass()))
		{
			LOGGER.trace("|{}++ {}: recursive context", tabs, ctxClass);
			return parseGenericParam(ctx.getChild(ParserRuleContext.class, 0), currentGroupBy, level, maybeNullParam);
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

	private ParameterType parseTypeParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Typeparam param)
	{
		ParserRuleContext typeContext = resolveRecursiveContext(getFieldOrMethod(param, ctx, ParserRuleContext.class, level), level);
		
		if (typeContext == null)
			return null;
		else if (scalarParamClass.isInstance(typeContext))
		{
			VTLAlias domainName = parseAliasParam(typeContext, null, level + 1, scalarParam.getTyperule());
			ScalarValue<?, ?, ?, ?> defaultValue = parseValueParam(typeContext, null, level + 1, scalarParam.getDefaultrule());
			
			return new ScalarParameterTypeImpl(domainName, defaultValue);
		}
		else if (componentParamClass.isInstance(typeContext))
		{
			ScalarParameterType scalarData = (ScalarParameterType) parseTypeParam(typeContext, null, level + 1, componentParam.getScalarrule());
			Role role = Role.from(parseRoleParam(typeContext, null, level + 1, componentParam.getRolerule()));
			
			if (scalarData == null)
				return new ComponentParameterTypeImpl(role, null, null);
			else
				return new ComponentParameterTypeImpl(role, scalarData.getDomainName(), scalarData.getDefaultValue());
		}
		else if (datasetParamClass.isInstance(typeContext))
		{
			List<?> componentList = getFieldOrMethod(new Listparam(null, datasetParam.getListrule(), null, param), typeContext, List.class, level + 1);
			List<QuantifiedComponent> paramDefs = componentList.stream()
					.map(prc -> resolveRecursiveContext((ParserRuleContext) prc, level))
					.map(constraintCtx -> { 
						try {
							ComponentParameterType componentData = (ComponentParameterType) parseTypeParam(constraintCtx, null, level + 1, datasetParam.getComponentrule());
							
							VTLAlias compName = parseAliasParam(constraintCtx, currentGroupBy, level + 1, datasetParam.getNamerule());
							Object maybeQuantifier = parseGenericParam(constraintCtx, currentGroupBy, level + 1, datasetParam.getQuantifierrule());
							Multiplicity quantifier = (Multiplicity) (maybeQuantifier instanceof Collection ? ((Collection<?>) maybeQuantifier).iterator().next() : maybeQuantifier);
							
							return new QuantifiedComponent(componentData.getRole(), componentData.getDomainName(), compName, quantifier);
						} catch (Exception e) {
							throw new VTLNestedException("in extracting dataset parameter component: " + constraintCtx.getText(), e);
						}
					}).collect(toList());
			
			return new DataSetParameterTypeImpl(paramDefs);
		}
		else
			throw new UnsupportedOperationException("Parameter of type " + typeContext.getClass().getSimpleName() + " not mapped.");
	}
	
	private Parameter parseParamParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Paramparam param)
	{
		ParserRuleContext paramCtx = getFieldOrMethod(param, ctx, ParserRuleContext.class, level);
		VTLAlias paramName = parseAliasParam(paramCtx, null, level + 1, param.getParamname());
		ParameterType parameterType = parseTypeParam(paramCtx, currentGroupBy, level, param.getParamtype());

		return new ParameterImpl(paramName, parameterType);
	}

	private Object parseCustomParam(ParserRuleContext ctx, GroupingClause currentGroupBy, int level, Customparam customParam)
	{
		ParserRuleContext customCtx = null;
		List<Param> innerParams = null;
		List<Object> args = null;
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
			
			args = new ArrayList<>(innerParams.size());
			for (Param child : innerParams)
			{
				Object arg = parseGenericParam(customCtx, currentGroupBy, level + 1, child);
				if (child instanceof Nestedparam)
					args.addAll((Collection<?>) arg);
				else
					args.add(arg);
			}
			
			customClass = Class.forName(customParam.getClazz());
			if (customParam.getMethod() != null)
				return Arrays.stream(customClass.getMethods())
						.filter(m -> m.getName().equals(customParam.getMethod()))
						.findAny()
						.orElseThrow(() -> new NoSuchMethodException(customParam.getMethod()))
						.invoke(null, args.toArray());
			else
				for (Constructor<?> ctor: customClass.getConstructors())
				{					
					Class<?>[] types = ctor.getParameterTypes();
					boolean bad = types.length != args.size();
					for (int i = 0; !bad && i < ctor.getParameterCount(); i++)
					{
						Object ith = args.get(i);
						if (ith != null && !types[i].isAssignableFrom(ith.getClass()))
							bad = true;
					}
					
					if (!bad)
						return ctor.newInstance(args.toArray());
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
			if (nestedCtx == null)
				resultList.add(null);
			else
			{
				Object arg = parseGenericParam(nestedCtx, currentGroupBy, level + 1, child);
				if (child instanceof Nestedparam)
					resultList.addAll((Collection<?>) arg);
				else
					resultList.add(arg);
			}
		
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
			Object key = parseGenericParam(entry, currentGroupBy, level + 1, keyParam);
			Object value = parseGenericParam(entry, currentGroupBy, level + 1, valueParam);
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
			resultList.add(parseGenericParam(child, currentGroupBy, level + 1, insideParam));
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

	private VTLAlias parseAliasParam(ParserRuleContext ctx, Object currentGroupBy, int level, Aliasparam aliasparam)
	{
		String result;
		Object value = getFieldOrMethod(aliasparam, ctx, Object.class, level);
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
		
		if (!firstToken.isPresent())
			return null;
		else
			return getRoleFromToken(firstToken.get()).roleClass();
	}

	private Role getRoleFromToken(Token token)
	{
		Role role = roleTokens.get(token.getType());
		if (role == null)
			throw new IllegalStateException("Unrecognized role token " + vocabulary.getSymbolicName(token.getType()) 
					+ " containing " + token.getText());
		else
			return role;
	}

	private ScalarValue<?, ?, ?, ?> parseValueParam(ParserRuleContext ctx, Object currentGroupBy, int level, Valueparam param)
	{
		// lookup actual token
		ParseTree element = getFieldOrMethod(param, ctx, ParseTree.class, level);
		if (element == null)
			return null;
		
		if (element instanceof RuleNode)
			element = resolveRecursiveContext((ParserRuleContext) element, level);
		
		String text = element.getText();
		SerFunction<String, ? extends ScalarValue<?, ?, ?, ?>> builder = valueBuilders.get(element.getClass());
		if (builder != null)
			return builder.apply(text);
		else if (element instanceof TerminalNode)
			return StringValue.of(text.matches("^\".*\"$") ? text.substring(1, text.length() - 1) : text);
		else if (element instanceof LimitClauseItemContext)
		{
			if (((LimitClauseItemContext) element).UNBOUNDED() != null) 
				return IntegerValue.of((long) (Integer.MAX_VALUE));
			if (((LimitClauseItemContext) element).CURRENT() != null) 
				return IntegerValue.of(0L);

			return IntegerValue.of(Long.parseLong(element.getChild(0).getText()));
		}
		else
			throw new IllegalStateException("Invalid context for valueparam: " + element.getClass());
	}

	private ParserRuleContext resolveRecursiveContext(ParserRuleContext ctx, int level)
	{
		if (ctx == null)
			return ctx;
		
		String tabs = new String(new char[level]).replace("\0", "    ");
		Class<? extends ParserRuleContext> oldClass;
		while (recursivecontexts.contains(ctx.getClass()) && !isNull(ctx.getChild(ParserRuleContext.class, 0)))
		{
			oldClass = ctx.getClass();
			LOGGER.trace("|{}>> Resolving recursive context {}", tabs, oldClass.getSimpleName());
			ctx = ctx.getChild(ParserRuleContext.class, 0);
			LOGGER.trace("|{}<< Recursive context {} yield {}", tabs, oldClass.getSimpleName(), ctx.getClass().getSimpleName());
		}
		
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
					String sourceToken = vocabulary.getSymbolicName(((Token) rule).getType());
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
		
		Object result = ctx;
		if (param.getName() != null)
//			result = getFieldOrMethodFromName(param.getName(), ctx, resultClass, level);
		{
			List<Exception> suppressed = new ArrayList<>();
			List<String> names = asList(param.getName().split("\\|"));
			Class<? extends RuleContext> ctxClass = ctx.getClass();
			
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
			String sourceToken = vocabulary.getSymbolicName(token.getType());
			LOGGER.trace("|{}<< Found token {} with value '{}'", tabs, sourceToken, token.getText());
		}
		else if (result instanceof TerminalNode)
		{
			Token token = (Token) ((TerminalNode) result).getPayload();
			String sourceToken = vocabulary.getSymbolicName(token.getType());
			LOGGER.trace("|{}<< Found token {} with value '{}'", tabs, sourceToken, token.getText());
		}
		else if (result != null)
			LOGGER.trace("|{}<< Found result {} with value {}", tabs, result.getClass(), result);
		else
			LOGGER.trace("|{}<< Found null result", tabs);

		return resultClass.cast(result);
	}
}
