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
/*******************************************************************************
7 * Copyright 2020, Bank Of Italy
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
package it.bancaditalia.oss.vtl.impl.transform.string;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class ReplaceTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	
	private final Transformation exprOperand;
	private final Transformation patternOperand;
	private final Transformation replaceOperand;

	public ReplaceTransformation(Transformation expr, Transformation pattern, Transformation replace)
	{
		exprOperand = expr;
		patternOperand = pattern;
		this.replaceOperand = replace == null ? new ConstantOperand<>(new StringValue("")) : replace;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue left = exprOperand.eval(session);
		ScalarValue<?, ? extends StringDomainSubset, StringDomain> pattern = STRINGDS.cast((ScalarValue<?, ?, ?>) patternOperand.eval(session));
		ScalarValue<?, ? extends StringDomainSubset, StringDomain> replace = STRINGDS.cast((ScalarValue<?, ?, ?>) replaceOperand.eval(session));
		
		if (left instanceof DataSet)
		{
			DataSet dataset = (DataSet) left;
			DataSetMetadata structure = dataset.getMetadata();
			Set<DataStructureComponent<Measure,?,?>> measures = dataset.getComponents(Measure.class);
			Pattern compiled = pattern instanceof NullValue ? null : Pattern.compile(STRINGDS.cast(pattern).get().toString());
			
			return dataset.mapKeepingKeys(structure, dp -> measures.stream()
				.map(measure -> {
					ScalarValue<?, ? extends StringDomainSubset, StringDomain> scalar = STRINGDS.cast(dp.get(measure));
					if (pattern == null || scalar instanceof NullValue)
						return new SimpleEntry<>(measure, STRINGDS.cast(NullValue.instance(STRINGDS)));
					
					return new SimpleEntry<>(measure, new StringValue(compiled.matcher(scalar.get().toString()).replaceAll(replace.get().toString())));
				}).collect(Utils.entriesToMap())
			); 
		}
		else
		{
			ScalarValue<?, ?, ?> scalar = (ScalarValue<?, ?, ?>) left;
			if (left instanceof NullValue || pattern instanceof NullValue)
				return NullValue.instance(STRINGDS);
			
			Pattern compiled = Pattern.compile(STRINGDS.cast(pattern).get().toString());
			return new StringValue(compiled.matcher(scalar.get().toString()).replaceAll(replace.get().toString()));
		}
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata source = exprOperand.getMetadata(session), pattern = patternOperand.getMetadata(session),
				replace = replaceOperand.getMetadata(session);
		
		if (!(pattern instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(pattern, DataSetMetadata.class);
		if (!(replace instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(replace, DataSetMetadata.class);
		if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?>) pattern).getDomain()))
			throw new VTLIncompatibleTypesException("replace: pattern parameter", STRING, ((ScalarValueMetadata<?>) pattern).getDomain());
		if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?>) replace).getDomain()))
			throw new VTLIncompatibleTypesException("replace: replacement parameter", STRING, ((ScalarValueMetadata<?>) replace).getDomain());
		
		if (source instanceof ScalarValueMetadata)
		{
			ScalarValueMetadata<?> leftV = (ScalarValueMetadata<?>) source; 
			if (!(STRING.isAssignableFrom(leftV.getDomain())))
				throw new VTLIncompatibleTypesException("replace", STRING, leftV.getDomain());
			else
				return STRING;
		}
		else 
		{
			DataSetMetadata metadata = (DataSetMetadata) source;
			
			final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = metadata.getComponents(Measure.class);
			measures.stream().forEach(c -> {
				if (!STRINGDS.isAssignableFrom(c.getDomain()))
					throw new VTLIncompatibleTypesException("replace", c, STRINGDS);
			});
			
			return metadata;
		}
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		Set<LeafTransformation> terminals = new HashSet<>();
		terminals.addAll(exprOperand.getTerminals());
		terminals.addAll(patternOperand.getTerminals());
		terminals.addAll(replaceOperand.getTerminals());
		return terminals;
	}
	
	@Override
	public String toString()
	{
		return "replace(" + exprOperand + ", " + patternOperand + ", " + replaceOperand + ")"; 
	}
}
