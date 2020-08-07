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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptyList;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.grammar.Vtl;
import it.bancaditalia.oss.vtl.grammar.Vtl.ComponentTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefOperatorContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefOperatorsContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefineExpressionContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.InputParameterTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.OutputParameterTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ParameterItemContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.PersistAssignmentContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ScalarTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.StatementContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.TemporaryAssignmentContext;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedContextException;
import it.bancaditalia.oss.vtl.impl.engine.mapping.OpsFactory;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.ViralAttribute;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

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
//		else if (node instanceof DefineExpressionContext)
//		{
//			DefHierarchicalContext dhc = ((DefineHierarchicalStatementContext) node).defHierarchical();
//			String name = dhc.rulesetID().getText();
//			boolean isValueDomain = dhc.hierRuleSignature().VALUE_DOMAIN() != null; 
//			String item = dhc.hierRuleSignature().ruleName.getText(); 
//			Map<String, String> conditions = dhc.hierRuleSignature().conditioningItem() == null ? Collections.emptyMap() 
//					: dhc.hierRuleSignature().conditioningItem().stream()
//						.collect(toMap(i -> i.cond.getText(), i -> i.alias != null ? i.alias.getText() : i.cond.getText()));
//			
//			List<RuleItemImpl> rules = dhc.ruleClauseHierarchical().ruleItemHierarchical().stream()
//				.map(rihc -> new RuleItemImpl(rihc.codeItemRelation().IDENTIFIER().getText(),
//						mapCO(rihc.codeItemRelation().opComp),
//						rihc.codeItemRelation().codeItemRelationClause().stream()
//							.map(circ -> new SourceItemImpl(circ.IDENTIFIER().getText(), circ.DASH() == null, buildExpr(circ.expr())))
//							.collect(toList()),
//						buildExpr(rihc.codeItemRelation().expr()), buildExpr(rihc.erCode().constant()), buildExpr(rihc.erLevel())))
//				.collect(toList());
//			
//			return new DefineHierarchyStatement(name, item, isValueDomain, conditions, rules);
//		}
		else
		{
			DefOperatorsContext defineContext = ((DefineExpressionContext) ctx).defOperators();
			if (defineContext instanceof DefOperatorContext)
			{
				DefOperatorContext defineOp = (DefOperatorContext) defineContext;
				List<Parameter> params = coalesce(defineOp.parameterItem(), emptyList()).stream()
						.map(this::buildParam)
						.collect(Collectors.toList());
				String outputType = buildParamType(defineOp.outputParameterType());
				
				return new DefineOperatorStatement(defineOp.operatorID().getText(), params, outputType, buildExpr(defineOp.expr()));
			}
			else
				throw new VTLUnmappedContextException(ctx);
		}
	}

	private Transformation buildExpr(ParserRuleContext ctx) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException
	{
		return opsFactory.buildExpr(ctx);
	}

	public Parameter buildParam(ParameterItemContext param)
	{
		return buildParamType(param.varID().getText(), param.inputParameterType());
//		
//		if (String.class == buildParamType.getValue())
//			return new ScalarParameter(, buildParamType.getKey());
//		else
//			throw new UnsupportedOperationException("Parameter type not implemented: " + param.inputParameterType().getText());
	}

	private Parameter buildParamType(String name, ParserRuleContext type)
	{
		ScalarTypeContext scalarType = null;
		ComponentTypeContext compType = null;
		if (type instanceof InputParameterTypeContext)
		{
			InputParameterTypeContext inputParameterTypeContext = (InputParameterTypeContext) type;
			scalarType = inputParameterTypeContext.scalarType();
			compType = inputParameterTypeContext.componentType();
		}
		else if (type instanceof ScalarTypeContext)
		{
			scalarType = (ScalarTypeContext) type;
		}
		else
			throw new UnsupportedOperationException(type.getClass().getName());
		
		if (scalarType != null)
		{
			ParserRuleContext scalarTypeName = coalesce(scalarType.basicScalarType(), scalarType.valueDomainName());
			if (scalarType.scalarTypeConstraint() != null)
				throw new UnsupportedOperationException("Domain constraint not implemented.");
			if (scalarType.NULL_CONSTANT() != null)
				throw new UnsupportedOperationException("NULL/NOT NULL constraint not implemented.");
			return new ScalarParameter(name, scalarTypeName.getText());
		}
		else if (compType != null)
		{
			String domain = null;
			scalarType = compType.scalarType();
			if (scalarType != null)
				domain = ((ScalarParameter) buildParamType(name, scalarType)).getDomain();

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

			Class<? extends ComponentRole> role;
			if (resultList.size() == 0)
				role = null;
			else if (resultList.size() == 1 && resultList.get(0).getType() == Vtl.MEASURE)
				role = Measure.class;
			else if (resultList.size() == 1 && resultList.get(0).getType() == Vtl.DIMENSION)
				role = Identifier.class;
			else if (resultList.size() == 1 && resultList.get(0).getType() == Vtl.ATTRIBUTE)
				role = Attribute.class;
			else if (resultList.size() == 2 && resultList.get(0).getType() == Vtl.VIRAL && resultList.get(1).getType() == Vtl.ATTRIBUTE)
				role = ViralAttribute.class;
			else
			{
				Token token = resultList.get(0);
				throw new IllegalStateException("Unrecognized role token " + Vtl.VOCABULARY.getSymbolicName(token.getType()) + " containing " + token.getText());
			}
			
			return new ComponentParameter<>(name, domain, role);
		}
		else
			throw new UnsupportedOperationException("Parameter of type " + type.getText() + " not implemented.");
	}

	private String buildParamType(OutputParameterTypeContext type)
	{
		if (type == null)
			return null;
		else if (type.scalarType() != null)
		{
			ScalarTypeContext scalarType = type.scalarType();
			ParserRuleContext scalarTypeName = coalesce(scalarType.basicScalarType(), scalarType.valueDomainName());
			if (scalarType.scalarTypeConstraint() != null)
				throw new UnsupportedOperationException("Domain constraint not implemented.");
			if (scalarType.NULL_CONSTANT() != null)
				throw new UnsupportedOperationException("NULL/NOT NULL constraint not implemented.");
			return scalarTypeName.getText();
		}
		else
			throw new UnsupportedOperationException("Parameter of type " + type.getText() + " not implemented.");
	}
}
