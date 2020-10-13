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

import static it.bancaditalia.oss.vtl.impl.engine.statement.AnonymousComponentConstraint.QuantifierConstraints.ANY;
import static it.bancaditalia.oss.vtl.impl.engine.statement.AnonymousComponentConstraint.QuantifierConstraints.AT_LEAST_ONE;
import static it.bancaditalia.oss.vtl.impl.engine.statement.AnonymousComponentConstraint.QuantifierConstraints.MAX_ONE;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
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
import it.bancaditalia.oss.vtl.grammar.Vtl.CompConstraintContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ComponentTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DatasetTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefOperatorContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefOperatorsContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.DefineExpressionContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.InputParameterTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.MultModifierContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.OutputParameterTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ParameterItemContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.PersistAssignmentContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.ScalarTypeContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.StatementContext;
import it.bancaditalia.oss.vtl.grammar.Vtl.TemporaryAssignmentContext;
import it.bancaditalia.oss.vtl.impl.engine.exceptions.VTLUnmappedContextException;
import it.bancaditalia.oss.vtl.impl.engine.mapping.OpsFactory;
import it.bancaditalia.oss.vtl.impl.engine.statement.AnonymousComponentConstraint.QuantifierConstraints;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
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
				Parameter outputType = buildParamType(defineOp.outputParameterType());
				
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

	private static Parameter buildParamType(String name, ParserRuleContext type)
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
		
		// check kind of each parameter used in the operator definition and generate constraints
		if (scalarType != null)
			return new ScalarParameter(name, generateDomainName(scalarType));
		else if (compType != null)
		{
			Entry<Class<? extends Component>, String> metadata = generateComponentMetadata(compType);
			return new ComponentParameter<>(name, metadata.getValue(), metadata.getKey());
		}
		else if (datasetType != null)
			return new DataSetParameter(name, datasetType.compConstraint().stream()
					.map(StatementFactory::generateComponentConstraint)
					.collect(toList()));
		else
			throw new UnsupportedOperationException("Parameter of type " + type.getText() + " not implemented.");
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
		final Entry<Class<? extends Component>, String> metadata = generateComponentMetadata(constraint.componentType());
		
		if (constraint.componentID() != null)
			return new NamedComponentConstraint(constraint.componentID().getText(), metadata.getKey(), metadata.getValue());
		else
			return new AnonymousComponentConstraint(metadata.getKey(), metadata.getValue(), generateMultConstraint(constraint.multModifier()));			
	}

	private static QuantifierConstraints generateMultConstraint(MultModifierContext multModifier)
	{
		if (multModifier.PLUS() != null)
			return AT_LEAST_ONE;
		else if (multModifier.MUL() != null)
			return ANY;
		else
			return MAX_ONE;
	}

	private static Entry<Class<? extends Component>, String> generateComponentMetadata(ComponentTypeContext compType)
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

		Class<? extends Component> role;
		if (resultList.size() == 0)
			role = null;
		else if (resultList.size() == 1 && resultList.get(0).getType() == Vtl.COMPONENT)
			role = Component.class;
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
		
		return new SimpleEntry<>(role, domain);
	}

	private Parameter buildParamType(OutputParameterTypeContext type)
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
			return new ScalarParameter(null, scalarTypeName.getText());
		}
		else
			throw new UnsupportedOperationException("Parameter of type " + type.getText() + " not implemented.");
	}
}
