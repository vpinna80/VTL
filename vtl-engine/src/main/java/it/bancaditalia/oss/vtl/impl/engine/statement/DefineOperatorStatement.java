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

import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.util.List;

import it.bancaditalia.oss.vtl.engine.UserOperatorStatement;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.Parameter;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

class DefineOperatorStatement implements UserOperatorStatement, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias alias;
	private final Transformation expression;
	private final List<Parameter> params;
	private final Parameter resultType;

	public DefineOperatorStatement(VTLAlias alias, List<Parameter> params, Parameter resultType, Transformation expression)
	{
		this.alias = alias;
		this.expression = expression;
		this.params = params;
		this.resultType = resultType;
	}
	
	@Override
	public boolean isCacheable()
	{
		return false;
	}
	
	@Override
	public String toString()
	{
		return "define operator " + getAlias() + "(" + params.stream().map(Parameter::toString).collect(joining(", ")) + ")"
				+ (resultType != null ? " returns " + resultType : "") + " is " + expression + " end operator;";
	}

	@Override
	public List<Parameter> getParameters()
	{
		return params;
	}

	@Override
	public Transformation getExpression()
	{
		return expression;
	}

	@Override
	public VTLAlias getAlias()
	{
		return alias;
	}
}