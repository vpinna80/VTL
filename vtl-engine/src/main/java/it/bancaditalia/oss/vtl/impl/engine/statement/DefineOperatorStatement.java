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
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.NamedOperator;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

class DefineOperatorStatement extends AbstractStatement implements NamedOperator
{
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(DefineOperatorStatement.class);
	private static final long serialVersionUID = 1L;

	private final Transformation	expression;
	private final List<Parameter> params;
	private final Parameter resultType;
	private final Map<String, Parameter> paramMap;

	public DefineOperatorStatement(String name, List<Parameter> params, Parameter resultType, Transformation expression)
	{
		super(name);
		
		this.expression = expression;
		this.params = params;
		this.resultType = resultType;
		this.paramMap = params.stream().collect(Utils.toMapWithKeys(Parameter::getName));
	}

	public Transformation getExpression()
	{
		return expression;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return expression.getTerminals();
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		try
		{
			return expression.eval(scheme);
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("Error evaluating statement '" + this + "'", e);
		}
	}
	
	@Override
	public VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		MetadataRepository repo = scheme.getRepository();

		TransformationScheme decoratedScheme = Utils.getStream(params).map(Parameter::getName).allMatch(scheme::contains)
			? scheme
			: new TransformationScheme() {
				
				@Override
				public VTLValue resolve(String alias)
				{
					return scheme.resolve(alias);
				}
				
				@Override
				public Statement getRule(String alias)
				{
					return scheme.getRule(alias);
				}
				
				@Override
				public MetadataRepository getRepository()
				{
					return scheme.getRepository();
				}
				
				@Override
				public VTLValueMetadata getMetadata(String alias)
				{
					return paramMap.containsKey(alias) ? UnknownValueMetadata.INSTANCE : scheme.getMetadata(alias);
				}
				
				@Override
				public boolean contains(String alias)
				{
					return paramMap.containsKey(alias) || scheme.contains(alias);
				}
			};
		
		// check if each parameter metadata matches its definition			
		for (Parameter param: params)
		{
			VTLValueMetadata actualParamMeta = decoratedScheme.getMetadata(param.getName());
			
			if (param instanceof ScalarParameter)
			{
				String declaredDomain = ((ScalarParameter) param).getDomain();

				// Scalar or component parameter
				if (param.matches(actualParamMeta))
					matchDomains(param.getName(), declaredDomain, (ScalarValueMetadata<?>) actualParamMeta, repo);
				// when inside a bracket expression, a component is a monomeasure dataset
				else if (decoratedScheme.getParent().getParent() != null)
				{
					Set<DataStructureComponent<Measure, ?, ?>> measures = ((DataSetMetadata) actualParamMeta).getComponents(Measure.class);
					// This condition should never happen, but better to have a check
					if (measures.size() != 1)
						throw new VTLException(getId() + ": a " + param.getMetaString() + " was expected for parameter '" 
								+ param.getName() + "' but " + actualParamMeta + " was found.");
					else 
						matchDomains(param.getName(), declaredDomain, measures.iterator().next()::getDomain, repo);
				}
				else
					throw new VTLException(getId() + ": a " + param.getMetaString() + " was expected for parameter '" 
							+ param.getName() + "' but " + actualParamMeta + " was found.");
			}
			else if (param instanceof DataSetParameter)
			{
				if (!param.matches(actualParamMeta))
					throw new VTLException(getId() + ": a " + param.getMetaString() + " was expected for parameter '" + param.getName() 
					+ "' but " + actualParamMeta + " was found.");
					
			}
			else
				throw new UnsupportedOperationException(param.getClass().getSimpleName() + " not implemented.");
		}
		
		// get the actual result type of the expression
		VTLValueMetadata metadata = expression.getMetadata(decoratedScheme);
		
		if (!resultType.matches(metadata))
			throw new VTLException(getId() + ": a result of type " + resultType.getMetaString() + " was expected but "
					+ metadata + " was found.");

		return metadata;
	}

	private void matchDomains(String paramName, String expectedDomainName, ScalarValueMetadata<?> actualParamMeta, MetadataRepository repo)
	{
		ValueDomainSubset<?> expectedDomain = expectedDomainName == null ? null : repo.getDomain(expectedDomainName);
		if (expectedDomain != null)
		{
			ValueDomainSubset<?> actualDomain = ((ScalarValueMetadata<?>) actualParamMeta).getDomain();
			if (!expectedDomain.isAssignableFrom(actualDomain))
				throw new VTLIncompatibleTypesException("Parameter " + paramName, expectedDomain, actualDomain);
		}
	}

	@Override
	public List<String> getParameterNames()
	{
		return params.stream().map(Parameter::getName).collect(toList());
	}
	
	@Override
	public boolean isCacheable()
	{
		return false;
	}
	
	@Override
	public String toString()
	{
		return "define operator " + getId() + "(" + params.stream().map(Parameter::toString).collect(joining(", ")) + ")"
				+ (resultType != null ? " returns " + resultType : "") + " is " + expression + " end operator;";
	}
}
