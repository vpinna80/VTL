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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;

import java.util.HashSet;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLNonPositiveConstantException;
import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
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
		startOperand = start == null ? new ConstantOperand(IntegerValue.of(1L)) : start;
		lenOperand = len == null ? new ConstantOperand(NullValue.instance(INTEGERDS)) : len;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue expr = exprOperand.eval(session);
		ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> start = INTEGERDS.cast((ScalarValue<?, ?, ?, ?>) startOperand.eval(session));
		ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> len = INTEGERDS.cast((ScalarValue<?, ?, ?, ?>) lenOperand.eval(session));
		
		if (start instanceof IntegerValue && (Long) start.get() < 1)
			throw new VTLNonPositiveConstantException("substr", start);
		if (len instanceof IntegerValue && (Long) len.get() < 1)
			throw new VTLNonPositiveConstantException("substr", len);
		
		if (expr.isDataSet())
		{
			DataSet dataset = (DataSet) expr;
			DataSetStructure structure = dataset.getMetadata();
			Set<DataSetComponent<Measure, ?, ?>> measures = dataset.getMetadata().getMeasures();
			
			return dataset.mapKeepingKeys(structure, lineageEnricher(this), dp -> measures.stream()
					.map(toEntryWithValue(measure -> getSubstring(len, start, STRINGDS.cast(dp.get(measure)))))
					.collect(entriesToMap())
			); 
		}
		else
			return getSubstring(len, start, STRINGDS.cast(STRINGDS.cast((ScalarValue<?, ?, ?, ?>) expr)));
	}

	private static ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> getSubstring(
			ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> len, 
			ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> start,
			ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> scalar)
	{
		int startV = start.isNull() ? 1 : (int) (long) (Long) start.get();
		ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> result;
		if (scalar.isNull())
			result = STRINGDS.cast(NullValue.instance(STRINGDS));
		else
		{
			String string = scalar.get().toString();
		
			if (startV > string.length())
				result = StringValue.of("");
			else
			{
				Integer lenV = len.isNull() ? null : (int) (long) (Long) len.get() + startV - 1;
			
				if (lenV != null && lenV > string.length())
					lenV = string.length();
				result = StringValue.of(lenV == null ? string.substring(startV - 1) : string.substring(startV - 1, lenV));
			}
		}
		return result;
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata source = exprOperand.getMetadata(session), start = startOperand.getMetadata(session),
				len = lenOperand.getMetadata(session);
		
		if (!(!start.isDataSet()))
			throw new VTLInvalidParameterException(start, DataSetStructure.class);
		if (!(!len.isDataSet()))
			throw new VTLInvalidParameterException(len, DataSetStructure.class);
		if (!INTEGERDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) start).getDomain()))
			throw new VTLIncompatibleTypesException("substr: start parameter", STRINGDS, ((ScalarValueMetadata<?, ?>) start).getDomain());
		if (!INTEGERDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) len).getDomain()))
			throw new VTLIncompatibleTypesException("substr: len parameter", STRINGDS, ((ScalarValueMetadata<?, ?>) len).getDomain());
		
		if (!source.isDataSet())
		{
			ScalarValueMetadata<?, ?> scalar = (ScalarValueMetadata<?, ?>) source; 
			if (!(STRING.isAssignableFrom(scalar.getDomain())))
				throw new VTLIncompatibleTypesException("substr", STRINGDS, scalar.getDomain());
			else
				return STRING;
		}
		else 
		{
			DataSetStructure metadata = (DataSetStructure) source;
			
			Set<DataSetComponent<?, ?, ?>> invalid = metadata.getMeasures().stream()
				.filter(c -> !(c.getDomain() instanceof StringDomain))
				.collect(toSet());
			
			if (!invalid.isEmpty())
				throw new VTLIncompatibleTypesException("substr", invalid, STRINGDS);
			
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
