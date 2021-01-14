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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.Collections.singletonMap;

import java.util.HashSet;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLSyntaxException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
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

public class InStrTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponentImpl<Measure, IntegerDomainSubset, IntegerDomain> INT_MEASURE = new DataStructureComponentImpl<>(INTEGER.getDomain().getVarName(), Measure.class, INTEGERDS);
	
	private final Transformation leftOperand;
	private final Transformation rightOperand;
	private final Transformation startOperand;
	private final Transformation occurrenceOperand;

	public InStrTransformation(Transformation left, Transformation right, Transformation start, Transformation occurrence)
	{
		this.leftOperand = left;
		this.rightOperand = right;
		this.startOperand = start == null ? new ConstantOperand<>(new IntegerValue(1L)) : start;
		this.occurrenceOperand = occurrence == null ? new ConstantOperand<>(new IntegerValue(1L)) : occurrence;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue left = leftOperand.eval(session);
		ScalarValue<?, ? extends StringDomainSubset, StringDomain> right = STRINGDS.cast((ScalarValue<?, ?, ?>) rightOperand.eval(session));
		ScalarValue<?,? extends IntegerDomainSubset,IntegerDomain> start = INTEGERDS.cast((ScalarValue<?, ?, ?>) startOperand.eval(session));
		ScalarValue<?,? extends IntegerDomainSubset,IntegerDomain> occurrence = INTEGERDS.cast((ScalarValue<?, ?, ?>) occurrenceOperand.eval(session));
		
		int startPos = (int) (long) (Long) start.get() - 1;
		int nOcc = (int) (long) (Long) occurrence.get() - 1;
		
		if (startPos < 0)
			throw new VTLSyntaxException("instr: start parameter must be positive but it was " + (startPos + 1));
		if (nOcc < 0)
			throw new VTLSyntaxException("instr: occurrence parameter must be positive but it was " + (nOcc + 1));
		
		if (left instanceof DataSet)
		{
			DataSet dataset = (DataSet) left;
			DataSetMetadata structure = new DataStructureBuilder(dataset.getMetadata().getComponents(Identifier.class))
					.addComponent(INT_MEASURE)
					.build();
			DataStructureComponent<Measure, StringDomainSubset, StringDomain> measure = dataset.getComponents(Measure.class, STRINGDS).iterator().next();
			String pattern = right instanceof NullValue ? null : STRINGDS.cast(right).get().toString();
			
			return dataset.mapKeepingKeys(structure, dp -> singletonMap(INT_MEASURE, 
					(ScalarValue<?, IntegerDomainSubset, IntegerDomain>) (pattern == null 
						? NullValue.instance(INTEGERDS)
						: findOccurrence(STRINGDS.cast(dp.get(measure)).get().toString(), pattern, startPos, nOcc)))); 
		}
		else
		{
			ScalarValue<?, ?, ?> scalar = (ScalarValue<?, ?, ?>) left;
			if (left instanceof NullValue || right instanceof NullValue)
				return NullValue.instance(INTEGERDS);
			
			return findOccurrence(STRINGDS.cast(scalar).toString(), right.toString(), startPos, nOcc);
		}
	}
	
	private static IntegerValue findOccurrence(String string, String pattern, int startPos, int nOcc)
	{
		int index = string.indexOf(pattern, startPos);
		
		if (index < 0 || nOcc <= 0)
			return new IntegerValue((long) index + 1);

		return findOccurrence(string, pattern, index + 1, nOcc - 1);
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata left = leftOperand.getMetadata(session), right = rightOperand.getMetadata(session),
				start = startOperand.getMetadata(session), occurrence = occurrenceOperand.getMetadata(session);
		
		if (!(right instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(right, ScalarValueMetadata.class);
		if (!(start instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(start, ScalarValueMetadata.class);
		if (!(occurrence instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(occurrence, ScalarValueMetadata.class);
		if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?>) right).getDomain()))
			throw new VTLIncompatibleTypesException("concat: pattern parameter", STRING, ((ScalarValueMetadata<?>) right).getDomain());
		if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?>) start).getDomain()))
			throw new VTLIncompatibleTypesException("concat: start parameter", INTEGER, ((ScalarValueMetadata<?>) start).getDomain());
		if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?>) occurrence).getDomain()))
			throw new VTLIncompatibleTypesException("concat: occurrence parameter", INTEGER, ((ScalarValueMetadata<?>) occurrence).getDomain());
		
		if (left instanceof ScalarValueMetadata)
		{
			ScalarValueMetadata<?> leftV = (ScalarValueMetadata<?>) left; 
			if (!(STRING.isAssignableFrom(leftV.getDomain())))
				throw new VTLIncompatibleTypesException("instr", STRING, leftV.getDomain());
			else
				return INTEGER;
		}
		else 
		{
			DataSetMetadata metadata = (DataSetMetadata) left;
			ScalarValueMetadata<?> value = (ScalarValueMetadata<?>) right;
			
			if (!STRING.isAssignableFrom(value.getDomain()))
				throw new VTLIncompatibleTypesException("instr", STRING, value.getDomain());
			
			final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = metadata.getComponents(Measure.class);
			if (measures.size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, STRINGDS, measures);
			
			DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
			if (!STRING.isAssignableFrom(measure.getDomain()))
				throw new VTLExpectedComponentException(Measure.class, STRING, measures);
			
			return new DataStructureBuilder(metadata.getComponents(Identifier.class))
					.addComponent(INT_MEASURE)
					.build();
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
		terminals.addAll(leftOperand.getTerminals());
		terminals.addAll(rightOperand.getTerminals());
		terminals.addAll(startOperand.getTerminals());
		terminals.addAll(occurrenceOperand.getTerminals());
		return terminals;
	}
	
	@Override
	public String toString()
	{
		return "instr(" + leftOperand + ", " + rightOperand + 
				(startOperand != null ? ", " + startOperand : "") + 
				(occurrenceOperand != null ? ", " + occurrenceOperand : "") + 
				")";
	}
}
