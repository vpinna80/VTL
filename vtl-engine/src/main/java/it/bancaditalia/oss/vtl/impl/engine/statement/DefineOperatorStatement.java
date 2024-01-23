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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.NamedOperator;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

class DefineOperatorStatement extends AbstractStatement implements NamedOperator
{
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(DefineOperatorStatement.class);
	private static final long serialVersionUID = 1L;

	private class DecoratedTransformation implements Transformation
	{
		private static final long serialVersionUID = 1L;

		private final Transformation original;
		
		public DecoratedTransformation(Transformation original)
		{
			this.original = original;
		}

		public boolean isTerminal()
		{
			return original.isTerminal();
		}

		public Set<LeafTransformation> getTerminals()
		{
			return original.getTerminals();
		}

		public VTLValue eval(TransformationScheme scheme)
		{
			return original.eval(scheme);
		}
		
		public VTLValueMetadata getMetadata(TransformationScheme scheme)
		{
			MetadataRepository repo = scheme.getRepository();
			
			for (Parameter param: params)
			{
				VTLValueMetadata actualParamMeta = scheme.getMetadata(param.getName());
				
				if (!(actualParamMeta instanceof UnknownValueMetadata))
					if (param instanceof ScalarParameter)
					{
						String declaredDomain = ((ScalarParameter) param).getDomain();
		
						// Scalar or component parameter
						if (param.matches(actualParamMeta))
							matchDomains(param.getName(), declaredDomain, (ScalarValueMetadata<?, ?>) actualParamMeta, repo);
						// when inside a bracket expression, a component is a monomeasure dataset
						else if (scheme.getParent().getParent() != null)
						{
							Set<DataStructureComponent<Measure, ?, ?>> measures = ((DataSetMetadata) actualParamMeta).getMeasures();
							// This condition should never happen, but better to have a check
							if (measures.size() != 1)
								throw new VTLException(getAlias() + ": a " + param.getMetaString() + " was expected for parameter '" 
										+ param.getName() + "' but " + actualParamMeta + " was found.");
							else 
								matchDomains(param.getName(), declaredDomain, measures.iterator().next()::getDomain, repo);
						}
						else
							throw new VTLException(getAlias() + ": a " + param.getMetaString() + " was expected for parameter '" 
									+ param.getName() + "' but " + actualParamMeta + " was found.");
					}
					else if (param instanceof DataSetParameter)
					{
						if (!param.matches(actualParamMeta))
							throw new VTLException(getAlias() + ": a " + param.getMetaString() + " was expected for parameter '" + param.getName() 
							+ "' but " + actualParamMeta + " was found.");
							
					}
					else
						throw new UnsupportedOperationException(param.getClass().getSimpleName() + " not implemented.");
			}

			VTLValueMetadata metadata = expression.getMetadata(scheme);
			
			if (!resultType.matches(metadata))
				throw new VTLException(getAlias() + ": a result of type " + resultType.getMetaString() + " was expected but "
						+ metadata + " was found.");
	
			return metadata;
		}

		private <S extends ValueDomainSubset<S, D>, D extends ValueDomain> void matchDomains(String paramName, String expectedDomainName, ScalarValueMetadata<S, D> actualParamMeta, MetadataRepository repo)
		{
			ValueDomainSubset<?, ?> expectedDomain = expectedDomainName == null ? null : repo.getDomain(expectedDomainName);
			if (expectedDomain != null)
			{
				ValueDomainSubset<?, ?> actualDomain = ((ScalarValueMetadata<?, ?>) actualParamMeta).getDomain();
				if (!expectedDomain.isAssignableFrom(actualDomain))
					throw new VTLIncompatibleTypesException("parameter " + paramName, expectedDomain, actualDomain);
			}
		}
	}
	
	private final Transformation expression;
	private final List<Parameter> params;
	private final Parameter resultType;

	public DefineOperatorStatement(String name, List<Parameter> params, Parameter resultType, Transformation expression)
	{
		super(name);
		
		this.expression = new DecoratedTransformation(expression);
		this.params = params;
		this.resultType = resultType;
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
		return "define operator " + getAlias() + "(" + params.stream().map(Parameter::toString).collect(joining(", ")) + ")"
				+ (resultType != null ? " returns " + resultType : "") + " is " + expression + " end operator;";
	}

	@Override
	public Transformation getExpression()
	{
		return expression;
	}
}
