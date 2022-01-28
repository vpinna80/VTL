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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import it.bancaditalia.oss.vtl.engine.NamedOperator;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.scope.ParamScope;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class CallTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private final String operator;
	private final List<Transformation> params;

	public CallTransformation(String operator, List<Transformation> params)
	{
		this.operator = operator;
		this.params = params;
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return Utils.getStream(params).map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		Statement statement = scheme.getRule(operator);
		if (statement instanceof NamedOperator)
		{
			NamedOperator op = (NamedOperator) statement;
			List<String> parNames = op.getParameterNames();
			Map<String, Transformation> paramValues = IntStream.range(0, params.size()).boxed()
				.collect(toMap(i -> parNames.get(i), i -> params.get(i)));
			return op.eval(new ParamScope(scheme, paramValues));
		}
		else
			throw new UnsupportedOperationException("Operator " + operator + " is not defined.");
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		Statement statement = scheme.getRule(operator);
		if (statement instanceof NamedOperator)
		{
			NamedOperator op = (NamedOperator) statement;
			List<String> parNames = op.getParameterNames();
			
			if (params.size() != parNames.size())
				throw new UnsupportedOperationException(operator + " requires " + parNames.size() + " parameters but " + params.size() + " were provided.");
			
			Map<String, Transformation> paramValues = IntStream.range(0, params.size()).boxed()
					.collect(toMap(i -> parNames.get(i), i -> params.get(i)));
			
			return op.getMetadata(new ParamScope(scheme, paramValues));
		}
		else
			throw new UnsupportedOperationException("Operator " + operator + " is not defined.");
	}
	
	@Override
	public String toString()
	{
		return operator + params.stream().map(Transformation::toString).collect(joining(", ", "(", ")"));
	}
	
	@Override
	public Lineage computeLineage()
	{
		return LineageNode.of(this, params.stream().map(Transformation::getLineage).collect(toList()).toArray(new Lineage[0]));
	}
}
