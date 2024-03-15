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
package it.bancaditalia.oss.vtl.impl.transform.ops;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.engine.UserOperatorStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLParameterMismatchException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.scope.ParamScope;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Parameter;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CallTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private final String operator;
	private final List<Transformation> args;

	public CallTransformation(String operator, List<Transformation> args)
	{
		this.operator = operator;
		this.args = args;
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return args.stream().map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		Statement statement = scheme.getRule(operator);
		if (statement instanceof UserOperatorStatement)
		{
			UserOperatorStatement op = (UserOperatorStatement) statement;
			List<Parameter> parNames = op.getParameters();
			Map<String, Transformation> paramValues = IntStream.range(0, args.size()).boxed()
				.collect(toMap(i -> parNames.get(i).getAlias(), i -> args.get(i)));
			return op.getExpression().eval(new ParamScope(scheme, paramValues));
		}
		else
			throw new UnsupportedOperationException("Operator " + operator + " is not defined.");
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		Statement statement = scheme.getRule(operator);
		if (statement instanceof UserOperatorStatement)
		{
			UserOperatorStatement op = (UserOperatorStatement) statement;
			List<Parameter> params = op.getParameters();
			
			if (args.size() != params.size())
				throw new UnsupportedOperationException(operator + " requires " + params.size() + " parameters but " + args.size() + " were provided.");
			
			Map<String, Transformation> argValues = new HashMap<>();
			for (int i = 0; i < args.size(); i++)
			{
				Parameter param = params.get(i);
				Transformation argValue = args.get(i);
				if (!param.matches(scheme, argValue))
					throw new VTLParameterMismatchException(param, argValue.getMetadata(scheme));
				argValues.put(param.getAlias(), argValue);
			}
			
			return op.getExpression().getMetadata(new ParamScope(scheme, argValues));
		}
		else
			throw new UnsupportedOperationException("Operator " + operator + " is not defined.");
	}
	
	@Override
	public String toString()
	{
		return operator + args.stream().map(Transformation::toString).collect(joining(", ", "(", ")"));
	}
}
