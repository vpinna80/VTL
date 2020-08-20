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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLSyntaxException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class SubstrTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	
	private final Transformation exprOperand;
	private final Transformation startOperand;
	private final Transformation lenOperand;

	public SubstrTransformation(Transformation expr, Transformation start, Transformation len)
	{
		exprOperand = expr;
		startOperand = start == null ? new ConstantOperand<>(new IntegerValue(1L)) : start;
		this.lenOperand = len == null ? new ConstantOperand<>(NullValue.instance(INTEGERDS)) : len;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue expr = exprOperand.eval(session);
		ScalarValue<?,? extends IntegerDomainSubset,IntegerDomain> start = INTEGERDS.cast((ScalarValue<?, ?, ?>) startOperand.eval(session));
		ScalarValue<?,? extends IntegerDomainSubset,IntegerDomain> len = INTEGERDS.cast((ScalarValue<?, ?, ?>) lenOperand.eval(session));
		
		if (start instanceof IntegerValue && (Long) len.get() < 1)
			throw new VTLSyntaxException("substr: start parameter must be positive but it is " + start.get());
		if (len instanceof IntegerValue && (Long) len.get() < 1)
			throw new VTLSyntaxException("substr: length parameter must be positive but it is " + len.get());
		
		if (expr instanceof DataSet)
		{
			DataSet dataset = (DataSet) expr;
			VTLDataSetMetadata structure = dataset.getDataStructure();
			Set<DataStructureComponent<Measure, ?, ?>> measures = dataset.getComponents(Measure.class);
			int startV = start instanceof NullValue ? 1 : (int) (long) (Long) start.get();
			
			return dataset.mapKeepingKeys(structure, dp -> measures.stream()
				.map(measure -> {
					ScalarValue<?, ? extends StringDomainSubset, StringDomain> scalar = STRINGDS.cast(dp.get(measure));
					if (scalar instanceof NullValue)
						return new SimpleEntry<>(measure, STRINGDS.cast(NullValue.instance(STRINGDS)));
					String string = scalar.get().toString();
					
					if (startV > string.length())
						return new SimpleEntry<>(measure, new StringValue(""));
					Integer lenV = len instanceof NullValue ? null : (int) (long) (Long) len.get() + startV - 1;
					if (lenV != null && lenV > string.length())
						lenV = string.length();
					
					return new SimpleEntry<>(measure, new StringValue(lenV == null ? string.substring(startV - 1) : string.substring(startV - 1, lenV)));
				}).collect(Collectors.toMap(Entry::getKey, Entry::getValue))
			); 
		}
		else
		{
			ScalarValue<?, ?, ?> scalar = (ScalarValue<?, ?, ?>) expr;
			if (scalar instanceof NullValue)
				return NullValue.instance(STRINGDS);
			int startV = start instanceof NullValue ? 1 : (int) (long) (Long) start.get();
			String string = scalar.get().toString();
			
			if (startV > string.length())
				return new StringValue("");
			Integer lenV = len instanceof NullValue ? null : (int) (long) (Long) len.get();
			if (lenV != null && lenV + startV - 1 > string.length())
				lenV = string.length() - startV + 1;
			
			return new StringValue(lenV == null ? string.substring(startV - 1) : string.substring(startV - 1, lenV));
		}
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata source = exprOperand.getMetadata(session), start = startOperand.getMetadata(session),
				len = lenOperand.getMetadata(session);
		
		if (!(start instanceof VTLScalarValueMetadata))
			throw new VTLInvalidParameterException(start, VTLDataSetMetadata.class);
		if (!(len instanceof VTLScalarValueMetadata))
			throw new VTLInvalidParameterException(len, VTLDataSetMetadata.class);
		if (!INTEGERDS.isAssignableFrom(((VTLScalarValueMetadata<?>) start).getDomain()))
			throw new VTLIncompatibleTypesException("substr: start parameter", STRING, ((VTLScalarValueMetadata<?>) start).getDomain());
		if (!INTEGERDS.isAssignableFrom(((VTLScalarValueMetadata<?>) len).getDomain()))
			throw new VTLIncompatibleTypesException("substr: len parameter", STRING, ((VTLScalarValueMetadata<?>) len).getDomain());
		
		if (source instanceof VTLScalarValueMetadata)
		{
			VTLScalarValueMetadata<?> scalar = (VTLScalarValueMetadata<?>) source; 
			if (!(STRING.isAssignableFrom(scalar.getDomain())))
				throw new VTLIncompatibleTypesException("substr", STRING, scalar.getDomain());
			else
				return STRING;
		}
		else 
		{
			VTLDataSetMetadata metadata = (VTLDataSetMetadata) source;
			
			final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = metadata.getComponents(Measure.class);
			measures.stream().forEach(c -> {
				if (!STRINGDS.isAssignableFrom(c.getDomain()))
					throw new VTLIncompatibleTypesException("substr", c, STRINGDS);
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
		terminals.addAll(startOperand.getTerminals());
		terminals.addAll(lenOperand.getTerminals());
		return terminals;
	}

	@Override
	public String toString()
	{
		return "substr(" + exprOperand +  
				(startOperand != null ? ", " + startOperand : "") + 
				(lenOperand != null ? ", " + lenOperand : "") + 
				")";
	}
}
