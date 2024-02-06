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
import static it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.RuleSetType.VARIABLE;
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
import it.bancaditalia.oss.vtl.grammar.Vtl.DefHierarchicalContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefOperatorContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefOperatorsContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefineExpressionContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.InputParameterTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.MultModifierContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.OutputParameterTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ParameterItemContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.PersistAssignmentContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.RuleItemHierarchicalContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ScalarTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.StatementContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.TemporaryAssignmentContext;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedContextException;
import it.bancaditalia.oss.vtl.impl.engine.mapping.OpsFactory;
import it.bancaditalia.oss.vtl.impl.engine.statement.DataSetComponentConstraint.QuantifierConstraints;
import it.bancaditalia.oss.vtl.model.data.Component.Role;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.RuleSetType;
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
			return new AssignStatement(asc.varID().getText(), buildExpr(asc.expr()), false);
		}
		else if (ctx instanceof PersistAssignmentContext)
		{
			PersistAssignmentContext asc = (PersistAssignmentContext) ctx;
			return new AssignStatement(asc.varID().getText(), buildExpr(asc.expr()), true);
		}
		else
		{
			DefOperatorsContext defineContext = ((DefineExpressionContext) ctx).defOperators();
			if (defineContext instanceof DefOperatorContext)
			{
				DefOperatorContext defineOp = (DefOperatorContext) defineContext;
				List<Parameter> params = coalesce(defineOp.parameterItem(), emptyList()).stream()
						.map(this::buildParam)
						.collect(toList());
				BaseParameter outputType = buildResultType(defineOp.outputParameterType());
				
				return new DefineOperatorStatement(defineOp.operatorID().getText(), params, outputType, buildExpr(defineOp.expr()));
			}
			else if (defineContext instanceof DefHierarchicalContext)
			{
				DefHierarchicalContext defineHier = (DefHierarchicalContext) defineContext;
				String alias = defineHier.rulesetID().getText();
				RuleSetType rulesetType = defineHier.hierRuleSignature().VALUE_DOMAIN() != null ? VALUE_DOMAIN : VARIABLE;
				String ruleID = defineHier.hierRuleSignature().IDENTIFIER() != null ? defineHier.hierRuleSignature().IDENTIFIER().getText() : null;
				
				List<String> names = new ArrayList<>();
				List<String> leftOps = new ArrayList<>();
				List<RuleType> compOps = new ArrayList<>();
				List<Map<String, Boolean>> rightOps = new ArrayList<>();
				List<String> ercodes = new ArrayList<>();
				List<Long> erlevels = new ArrayList<>();
				
				for (int i = 0; defineHier.ruleClauseHierarchical().ruleItemHierarchical(i) != null; i++)
				{
					RuleItemHierarchicalContext rule = defineHier.ruleClauseHierarchical().ruleItemHierarchical(i);
					names.add(rule.ruleName != null ? rule.ruleName.getText() : null);
					ercodes.add(rule.erCode() != null ? rule.erCode().constant().getText() : null);
					erlevels.add(rule.erLevel() != null ? Long.parseLong(rule.erLevel().constant().getText()) : null);

					CodeItemRelationContext relation = rule.codeItemRelation();
					leftOps.add(relation.codeItemRef.getText());
					switch (relation.comparisonOperand().getStart().getType())
					{
						case Vtl.EQ: compOps.add(RuleType.EQ); break;
						case Vtl.LT: compOps.add(RuleType.LT); break;
						case Vtl.LE: compOps.add(RuleType.LE); break;
						case Vtl.MT: compOps.add(RuleType.GT); break;
						case Vtl.ME: compOps.add(RuleType.GE); break;
						default: throw new UnsupportedOperationException("Invalid operand in ruleset rule " + rule.ruleName.getText() + ": " + relation.comparisonOperand().getText());
					}
					rightOps.add(relation.codeItemRelationClause().stream()
						.collect(toMap(r -> r.rightCodeItem.getText(), r -> r.opAdd == null || r.opAdd.getType() == Vtl.PLUS)));
				}
				
				return new DefineHierarchyStatement(alias, rulesetType, ruleID, names, leftOps, compOps, rightOps, ercodes, erlevels);
			}
			else
				throw new VTLUnmappedContextException(ctx);
		}
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

	private static String generateDomainName(ScalarTypeContext scalarType)
	{
		ParserRuleContext scalarTypeName = coalesce(scalarType.basicScalarType(), scalarType.valueDomainName());
		if (scalarType.scalarTypeConstraint() != null)
			throw new UnsupportedOperationException("Domain constraint not implemented.");
		if (scalarType.NULL_CONSTANT() != null)
			throw new UnsupportedOperationException("NULL/NOT NULL constraint not implemented.");
		final String domainName = scalarTypeName.getText();
		return domainName;
	}

	private static DataSetComponentConstraint generateComponentConstraint(CompConstraintContext constraint)
	{
		final Entry<Role, String> metadata = generateComponentMetadata(constraint.componentType());
		
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

	private static Entry<Role, String> generateComponentMetadata(ComponentTypeContext compType)
	{
		ScalarTypeContext scalarType;
		String domain = null;
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

	private BaseParameter buildResultType(OutputParameterTypeContext type)
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
			return new ScalarParameter(name, generateDomainName(scalarType));
		else if (compType != null)
		{
			Entry<Role, String> metadata = generateComponentMetadata(compType);
			return new ComponentParameter<>(name, metadata.getValue(), metadata.getKey());
		}
		else if (datasetType != null)
			return new DataSetParameter(name, datasetType.compConstraint().stream()
					.map(StatementFactory::generateComponentConstraint)
					.collect(toList()));
		else
			throw new UnsupportedOperationException("Parameter of type " + type.getText() + " not implemented.");
	}
	
	
}
