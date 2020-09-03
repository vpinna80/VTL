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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Set;

import it.bancaditalia.oss.vtl.engine.NamedOperator;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

class DefineOperatorStatement extends AbstractStatement implements NamedOperator
{
	//private final static Logger LOGGER = LoggerFactory.getLogger(AssignStatement.class);
	private static final long serialVersionUID = 1L;

	private final Transformation	expression;
	private final List<Parameter> params;
	private final String resultType;

	public DefineOperatorStatement(String name, List<Parameter> params, String resultType, Transformation expression)
	{
		super(name);
		this.expression = expression;
		this.params = params;
		this.resultType = resultType;
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
	public VTLValue eval(TransformationScheme session)
	{
		try
		{
			return expression.eval(session);
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("Error evaluating statement '" + this + "'", e);
		}
	}
	
	@Override
	public VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		try
		{
			VTLValueMetadata metadata;
			MetadataRepository repo = scheme.getRepository();
			ValueDomainSubset<?> expectedResultType = resultType == null ? null : (ValueDomainSubset<?>) repo.getDomain(resultType);
			try
			{
				metadata = expression.getMetadata(scheme);
			}
			catch (VTLException e)
			{
				// actual parameters are not available, don't perform checks
				return (ScalarValueMetadata<?>) () -> expectedResultType;
			}
				
			// Check result type if declared
			if (!(metadata instanceof ScalarValueMetadata<?>))
				throw new UnsupportedOperationException("non-scalar result of operator not implemented.");
			else if (expectedResultType != null)
			{
				ValueDomainSubset<?> actualResultType = ((ScalarValueMetadata<?>) metadata).getDomain();
				if (!expectedResultType.isAssignableFrom(actualResultType))
					throw new VTLIncompatibleTypesException("Expected result of operator of type ", expectedResultType, actualResultType);
			}
			
			for (Parameter param: params)
				if (param instanceof ComponentParameter)
				{
					ComponentParameter<?> compParam = (ComponentParameter<?>) param;
					VTLValueMetadata actualParamMeta = scheme.getMetadata(param.getName());
					ValueDomainSubset<?> expectedDomain = compParam.getDomain() == null ? null : repo.getDomain(compParam.getDomain());
					if (actualParamMeta instanceof DataSetMetadata)
					{
						DataSetMetadata dsMeta = (DataSetMetadata) actualParamMeta;
						if (expectedDomain != null && dsMeta.getComponents(Measure.class, expectedDomain).size() != 1)
							throw new VTLSingletonComponentRequiredException(Measure.class, expectedDomain, 
									dsMeta.getComponents(Measure.class, expectedDomain));
						else if (expectedDomain == null && dsMeta.getComponents(Measure.class).size() != 1)
							throw new VTLSingletonComponentRequiredException(Measure.class, dsMeta.getComponents(Measure.class));
					}
					else if (expectedDomain != null)
					{
						ValueDomainSubset<?> actualDomain = ((ScalarValueMetadata<?>) actualParamMeta).getDomain();
						if (!expectedDomain.isAssignableFrom(actualDomain))
							throw new VTLIncompatibleTypesException("Parameter " + param.getName(), expectedDomain, actualDomain);
					}
				}
				else if (param instanceof ScalarParameter)
				{
					ScalarParameter scParam = (ScalarParameter) param;
					VTLValueMetadata actualParamMeta = scheme.getMetadata(param.getName());
					ValueDomainSubset<?> expectedDomain = repo.getDomain(scParam.getDomain());
					if (actualParamMeta instanceof DataSetMetadata)
					{
						DataSetMetadata dsMeta = (DataSetMetadata) actualParamMeta;
						if (dsMeta.getComponents(Measure.class, expectedDomain).size() != 1)
							throw new VTLSingletonComponentRequiredException(Measure.class, expectedDomain, dsMeta);
					}
					else 
					{
						ValueDomainSubset<?> actualDomain = ((ScalarValueMetadata<?>) actualParamMeta).getDomain();
						if (!expectedDomain.isAssignableFrom(actualDomain))
							throw new VTLIncompatibleTypesException("Parameter " + param.getName(), expectedDomain, actualDomain);
					}
				}
				else
					throw new UnsupportedOperationException(param.getClass().getSimpleName() + " not implemented.");
			
			return metadata;
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("Error evaluating statement '" + this + "'. Error: " + e.getMessage(), e);
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
				+ (resultType != null ? " returns " + resultType : "") + " is" + System.lineSeparator()
				+ "\t" + expression + System.lineSeparator()
				+ "end operator;";
	}
}
