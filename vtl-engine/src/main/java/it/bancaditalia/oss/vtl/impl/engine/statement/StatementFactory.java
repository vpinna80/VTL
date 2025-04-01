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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import static it.bancaditalia.oss.vtl.impl.engine.statement.DataSetComponentConstraint.QuantifierConstraints.ANY;
import static it.bancaditalia.oss.vtl.impl.engine.statement.DataSetComponentConstraint.QuantifierConstraints.AT_LEAST_ONE;
import static it.bancaditalia.oss.vtl.impl.engine.statement.DataSetComponentConstraint.QuantifierConstraints.MAX_ONE;
import static it.bancaditalia.oss.vtl.model.data.Component.Role.ATTRIBUTE;
import static it.bancaditalia.oss.vtl.model.data.Component.Role.COMPONENT;
import static it.bancaditalia.oss.vtl.model.data.Component.Role.IDENTIFIER;
import static it.bancaditalia.oss.vtl.model.data.Component.Role.MEASURE;
import static it.bancaditalia.oss.vtl.model.data.Component.Role.VIRAL_ATTRIBUTE;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VARIABLE;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.grammar.Vtl;
import it.bancaditalia.oss.vtl.grammar.Vtl.CodeItemRelationContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.CompConstraintContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ComponentIDContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ComponentTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DatasetTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefDatapointRulesetContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefHierarchicalContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefOperatorContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefOperatorsContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefineExpressionContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ExprComponentContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.InputParameterTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.MultModifierContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.OutputParameterTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ParameterItemContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.PersistAssignmentContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.RuleItemDatapointContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.RuleItemHierarchicalContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ScalarTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.StatementContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.TemporaryAssignmentContext;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedContextException;
import it.bancaditalia.oss.vtl.impl.engine.mapping.OpsFactory;
import it.bancaditalia.oss.vtl.impl.engine.statement.DataSetComponentConstraint.QuantifierConstraints;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Role;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType;
import it.bancaditalia.oss.vtl.model.transform.Parameter;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import jakarta.xml.bind.JAXBException;

public class StatementFactory implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final OpsFactory opsFactory;

	public StatementFactory() throws ClassNotFoundException, JAXBException, IOException
	{
		this.opsFactory = new OpsFactory();
	}

	public Statement createStatement(StatementContext ctx) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException
	{
		if (ctx instanceof TemporaryAssignmentContext)
		{
			TemporaryAssignmentContext asc = (TemporaryAssignmentContext) ctx;
			return new AssignStatement(VTLAliasImpl.of(asc.varID().getText()), buildExpr(asc.expr()), false);
		}
		else if (ctx instanceof PersistAssignmentContext)
		{
			PersistAssignmentContext asc = (PersistAssignmentContext) ctx;
			return new AssignStatement(VTLAliasImpl.of(asc.varID().getText()), buildExpr(asc.expr()), true);
		}
		else if (ctx instanceof DefineExpressionContext)
		{
			DefOperatorsContext defineContext = ((DefineExpressionContext) ctx).defOperators();
			if (defineContext instanceof DefOperatorContext)
			{
				DefOperatorContext defineOp = (DefOperatorContext) defineContext;
				List<Parameter> params = coalesce(defineOp.parameterItem(), emptyList()).stream()
						.map(this::buildParam)
						.collect(toList());
				BaseParameter outputType = buildResultType(defineOp.outputParameterType());
				
				return new DefineOperatorStatement(VTLAliasImpl.of(defineOp.operatorID().getText()), params, outputType, buildExpr(defineOp.expr()));
			}
			else if (defineContext instanceof DefHierarchicalContext)
			{
				DefHierarchicalContext defineHier = (DefHierarchicalContext) defineContext;
				VTLAlias alias = VTLAliasImpl.of(defineHier.rulesetID().getText());
				RuleSetType rulesetType = defineHier.hierRuleSignature().VALUE_DOMAIN() != null ? VALUE_DOMAIN : VARIABLE;
				VTLAlias ruleID = defineHier.hierRuleSignature().IDENTIFIER() != null ? VTLAliasImpl.of(defineHier.hierRuleSignature().IDENTIFIER().getText()) : null;
				
				List<VTLAlias> names = new ArrayList<>();
				List<String> leftOps = new ArrayList<>();
				List<Transformation> whenOps = new ArrayList<>();
				List<RuleType> compOps = new ArrayList<>();
				List<Map<String, Boolean>> rightOps = new ArrayList<>();
				List<String> ercodes = new ArrayList<>();
				List<Long> erlevels = new ArrayList<>();
				
				for (int i = 0; defineHier.ruleClauseHierarchical().ruleItemHierarchical(i) != null; i++)
				{
					RuleItemHierarchicalContext rule = defineHier.ruleClauseHierarchical().ruleItemHierarchical(i);
					names.add(rule.ruleName != null ? VTLAliasImpl.of(rule.ruleName.getText()) : null);
					String erCode = null;
					if (rule.erCode() != null)
					{
						erCode = rule.erCode().constant().getText();
						erCode = erCode.substring(1, erCode.length() - 1);
					}
					ercodes.add(erCode);
					erlevels.add(rule.erLevel() != null ? Long.parseLong(rule.erLevel().constant().getText()) : null);

					CodeItemRelationContext relation = rule.codeItemRelation();
					switch (relation.comparisonOperand().getStart().getType())
					{
						case Vtl.EQ: compOps.add(RuleType.EQ); break;
						case Vtl.LT: compOps.add(RuleType.LT); break;
						case Vtl.LE: compOps.add(RuleType.LE); break;
						case Vtl.MT: compOps.add(RuleType.GT); break;
						case Vtl.ME: compOps.add(RuleType.GE); break;
						default: throw new UnsupportedOperationException("Invalid operand in ruleset rule " + rule.ruleName.getText() + ": " + relation.comparisonOperand().getText());
					}
					
					ExprComponentContext when = relation.exprComponent();
					
					leftOps.add(relation.codetemRef.getText());
					whenOps.add(when != null ? buildExpr(when): null);
					rightOps.add(relation.codeItemRelationClause().stream()
						.collect(toMap(r -> r.rightCodeItem.getText(), r -> r.opAdd == null || r.opAdd.getType() == Vtl.PLUS)));
				}
				
				return new DefineHierarchyStatement(alias, rulesetType, ruleID, names, leftOps, compOps, whenOps, rightOps, ercodes, erlevels);
			}
			else if (defineContext instanceof DefDatapointRulesetContext)
			{
				DefDatapointRulesetContext defineDatapoint = (DefDatapointRulesetContext) defineContext;

				VTLAlias alias = VTLAliasImpl.of(defineDatapoint.rulesetID().getText());
				RuleSetType rulesetType = defineDatapoint.rulesetSignature().VALUE_DOMAIN() != null ? VALUE_DOMAIN : VARIABLE;
				List<Entry<VTLAlias, VTLAlias>> vars = defineDatapoint.rulesetSignature().signature().stream()
						.map(s -> new SimpleEntry<VTLAlias, VTLAlias>(VTLAliasImpl.of(s.varID().getText()), s.alias() != null ? VTLAliasImpl.of(s.alias().getText()) : null))
						.collect(toList());
				
				List<VTLAlias> ruleIDs = new ArrayList<>();
				List<Transformation> conds = new ArrayList<>();
				List<Transformation> exprs = new ArrayList<>();
				List<String> ercodes = new ArrayList<>();
				List<Long> erlevels = new ArrayList<>();

				for (RuleItemDatapointContext rule: defineDatapoint.ruleClauseDatapoint().ruleItemDatapoint())
				{
					ruleIDs.add(rule.ruleName != null ? VTLAliasImpl.of(rule.ruleName.getText()) : null);
					conds.add(rule.antecedentContiditon != null ? buildExpr(rule.antecedentContiditon) : null);
					exprs.add(buildExpr(rule.consequentCondition));
					ercodes.add(rule.erCode() != null ? rule.erCode().constant().getText() : null);
					erlevels.add(rule.erLevel() != null ? Long.parseLong(rule.erLevel().constant().getText()) : null);
				}

				return new DefineDataPointStatement(alias, rulesetType, vars, ruleIDs, conds, exprs, ercodes, erlevels);
			}
			else
				throw new VTLUnmappedContextException(defineContext);
		}
		else
			throw new VTLUnmappedContextException(ctx);
	}

	private Transformation buildExpr(ParserRuleContext ctx) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException
	{
		return opsFactory.buildExpr(ctx);
	}

	public BaseParameter buildParam(ParameterItemContext param)
	{
		return buildParamType(param.varID().getText(), param.inputParameterType());
	}

	private static BaseParameter buildParamType(String name, ParserRuleContext type)
	{
		ScalarTypeContext scalarType = null;
		ComponentTypeContext compType = null;
		DatasetTypeContext datasetType = null;

		if (type instanceof InputParameterTypeContext)
		{
			InputParameterTypeContext inputParameterTypeContext = (InputParameterTypeContext) type;
			scalarType = inputParameterTypeContext.scalarType();
			compType = inputParameterTypeContext.componentType();
			datasetType = inputParameterTypeContext.datasetType();
		}
		else if (type instanceof ScalarTypeContext)
			scalarType = (ScalarTypeContext) type;
		else
			throw new UnsupportedOperationException(type.getClass().getName());

		return buildType(type, name, scalarType, compType, datasetType);
	}

	private static VTLAlias generateDomainName(ScalarTypeContext scalarType)
	{
		ParserRuleContext scalarTypeName = coalesce(scalarType.basicScalarType(), scalarType.valueDomainName());
		if (scalarType.scalarTypeConstraint() != null)
			throw new UnsupportedOperationException("Domain constraint not implemented.");
		if (scalarType.NULL_CONSTANT() != null)
			throw new UnsupportedOperationException("NULL/NOT NULL constraint not implemented.");
		final String domainName = scalarTypeName.getText();
		return VTLAliasImpl.of(domainName);
	}

	private static DataSetComponentConstraint generateComponentConstraint(CompConstraintContext constraint)
	{
		final Entry<Role, VTLAlias> metadata = generateComponentMetadata(constraint.componentType());
		
		ComponentIDContext id = constraint.componentID();
		return new DataSetComponentConstraint(id != null ? id.getText() : null, metadata.getKey(), metadata.getValue(), generateMultConstraint(constraint.multModifier()));
	}

	private static QuantifierConstraints generateMultConstraint(MultModifierContext multModifier)
	{
		if (multModifier == null)
			return null;
		else if (multModifier.PLUS() != null)
			return AT_LEAST_ONE;
		else if (multModifier.MUL() != null)
			return ANY;
		else if (multModifier.OPTIONAL() != null)
			return MAX_ONE;
		else
			return null;
	}

	private static Entry<Role, VTLAlias> generateComponentMetadata(ComponentTypeContext compType)
	{
		ScalarTypeContext scalarType;
		VTLAlias domain = null;
		scalarType = compType.scalarType();
		if (scalarType != null)
			domain = generateDomainName(scalarType);

		ParseTree roleCtx = compType.componentRole();
		Stack<ParseTree> stack = new Stack<>();
		List<Token> resultList = new ArrayList<>();
		stack.push(roleCtx);
		while (!stack.isEmpty())
		{
			ParseTree current = stack.pop();
			if (current instanceof TerminalNode)
				resultList.add((Token) current.getPayload());
			else if (current instanceof RuleContext)
				for (int i = 0; i < current.getChildCount(); i++)
					stack.push(current.getChild(i));
			else if (current != null)
				throw new IllegalStateException("Unexpected ParseTree of " + current.getClass());
		}

		Role role;
		if (resultList.size() == 0)
			role = null;
		else if (resultList.size() == 1 && resultList.get(0).getType() == Vtl.COMPONENT)
			role = COMPONENT;
		else if (resultList.size() == 1 && resultList.get(0).getType() == Vtl.MEASURE)
			role = MEASURE;
		else if (resultList.size() == 1 && resultList.get(0).getType() == Vtl.DIMENSION)
			role = IDENTIFIER;
		else if (resultList.size() == 1 && resultList.get(0).getType() == Vtl.ATTRIBUTE)
			role = ATTRIBUTE;
		else if (resultList.size() == 2 && resultList.get(0).getType() == Vtl.VIRAL && resultList.get(1).getType() == Vtl.ATTRIBUTE)
			role = VIRAL_ATTRIBUTE;
		else
		{
			Token token = resultList.get(0);
			throw new IllegalStateException("Unrecognized role token " + Vtl.VOCABULARY.getSymbolicName(token.getType()) + " containing " + token.getText());
		}
		
		return new SimpleEntry<>(role, domain);
	}

	private static BaseParameter buildResultType(OutputParameterTypeContext type)
	{
		if (type == null)
			return null;
		else
			/* the result doesn't have a name */
			return buildType(type, null, type.scalarType(), type.componentType(), type.datasetType());
	}

	private static BaseParameter buildType(ParserRuleContext type, String name, ScalarTypeContext scalarType, ComponentTypeContext compType, DatasetTypeContext datasetType)
	{
		// check kind of each parameter used in the operator definition and generate constraints
		if (scalarType != null)
			return new ScalarParameter(VTLAliasImpl.of(name), generateDomainName(scalarType));
		else if (compType != null)
		{
			Entry<Role, VTLAlias> metadata = generateComponentMetadata(compType);
			return new ComponentParameter<>(VTLAliasImpl.of(name), metadata.getValue(), metadata.getKey());
		}
		else if (datasetType != null)
			return new DataSetParameter(VTLAliasImpl.of(name), datasetType.compConstraint().stream()
					.map(StatementFactory::generateComponentConstraint)
					.collect(toList()));
		else
			throw new UnsupportedOperationException("Parameter of type " + type.getText() + " not implemented.");
	}
}
