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
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class InStrTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponentImpl<Measure, EntireIntegerDomainSubset, IntegerDomain> INT_MEASURE = new DataStructureComponentImpl<>(INTEGER.getDomain().getVarName(), Measure.class, INTEGERDS);
	
	private final Transformation leftOperand;
	private final Transformation rightOperand;
	private final Transformation startOperand;
	private final Transformation occurrenceOperand;

	public InStrTransformation(Transformation left, Transformation right, Transformation start, Transformation occurrence)
	{
		this.leftOperand = left;
		this.rightOperand = right;
		this.startOperand = start == null ? new ConstantOperand(IntegerValue.of(1L)) : start;
		this.occurrenceOperand = occurrence == null ? new ConstantOperand(IntegerValue.of(1L)) : occurrence;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue left = leftOperand.eval(session);
		ScalarValue<?, ?, EntireStringDomainSubset, StringDomain> right = STRINGDS.cast((ScalarValue<?, ?, ?, ?>) rightOperand.eval(session));
		ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> start = INTEGERDS.cast((ScalarValue<?, ?, ?, ?>) startOperand.eval(session));
		ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> occurrence = INTEGERDS.cast((ScalarValue<?, ?, ?, ?>) occurrenceOperand.eval(session));
		
		int startPos = start instanceof NullValue ? 1 : (int) (long) (Long) start.get();
		int nOcc = occurrence instanceof NullValue ? 1 : (int) (long) (Long) occurrence.get();
		String pattern = right instanceof NullValue ? null : STRINGDS.cast(right).get().toString();
		
		if (startPos < 1)
			throw new VTLSyntaxException("instr: start parameter must be positive but it was " + (startPos));
		if (nOcc < 1)
			throw new VTLSyntaxException("instr: occurrence parameter must be positive but it was " + (nOcc));
		
		if (left instanceof DataSet)
		{
			DataSet dataset = (DataSet) left;
			DataSetMetadata structure = new DataStructureBuilder(dataset.getMetadata().getComponents(Identifier.class))
					.addComponent(INT_MEASURE)
					.build();
			DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain> measure = dataset.getComponents(Measure.class, STRINGDS).iterator().next();
			
			return dataset.mapKeepingKeys(structure, dp -> LineageNode.of(this, dp.getLineage(), rightOperand.getLineage()), dp -> singletonMap(INT_MEASURE, 
							instrScalar(dp.get(measure), pattern, startPos, nOcc))); 
		}
		else
			return instrScalar((ScalarValue<?, ?, ?, ?>) left, pattern, startPos, nOcc);
	}

	private ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> instrScalar(ScalarValue<?, ?, ?, ?> scalar,
			String pattern, int startPos, int nOcc)
	{
		if (pattern == null)
			return NullValue.instance(INTEGERDS);
		else
			return findOccurrence(STRINGDS.cast(scalar).get().toString(), pattern, startPos, nOcc);
	}
	
	private static ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> findOccurrence(String string, String pattern, int startPos, int nOcc)
	{
		int index = string.indexOf(pattern, startPos - 1) + 1;
		
		if (index < 1 || nOcc <= 1)
			return IntegerValue.of((long) index);

		return findOccurrence(string, pattern, index + 1, nOcc - 1);
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata left = leftOperand.getMetadata(session), right = rightOperand.getMetadata(session),
				start = startOperand.getMetadata(session), occurrence = occurrenceOperand.getMetadata(session);
		
		if (!(start instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(start, ScalarValueMetadata.class);
		if (!(occurrence instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(occurrence, ScalarValueMetadata.class);
		if (!INTEGER.isAssignableFrom(((ScalarValueMetadata<?, ?>) start).getDomain()))
			throw new VTLIncompatibleTypesException("instr: start parameter", INTEGER, ((ScalarValueMetadata<?, ?>) start).getDomain());
		if (!INTEGER.isAssignableFrom(((ScalarValueMetadata<?, ?>) occurrence).getDomain()))
			throw new VTLIncompatibleTypesException("instr: occurrence parameter", INTEGER, ((ScalarValueMetadata<?, ?>) occurrence).getDomain());
		
		if (left instanceof ScalarValueMetadata)
		{
			ScalarValueMetadata<?, ?> leftV = (ScalarValueMetadata<?, ?>) left; 

			// pattern must be a scalar too
			if (!(right instanceof ScalarValueMetadata))
				throw new VTLInvalidParameterException(right, ScalarValueMetadata.class);
			else if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) right).getDomain()))
				throw new VTLIncompatibleTypesException("instr: pattern parameter", STRING, ((ScalarValueMetadata<?, ?>) right).getDomain());

			if (!(STRING.isAssignableFrom(leftV.getDomain())))
				throw new VTLIncompatibleTypesException("instr", STRING, leftV.getDomain());
			else
				return INTEGER;
		}
		else 
		{
			DataSetMetadata metadata = (DataSetMetadata) left;
			
			if (right instanceof DataSetMetadata)
			{
				DataSetMetadata patternMetadata = (DataSetMetadata) right;
				final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> patternMeasures = patternMetadata.getComponents(Measure.class);
				if (patternMeasures.size() != 1)
					throw new VTLSingletonComponentRequiredException(Measure.class, STRINGDS, patternMeasures);
			}
			else if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) right).getDomain()))
				throw new VTLIncompatibleTypesException("instr: pattern parameter", STRING, ((ScalarValueMetadata<?, ?>) right).getDomain());

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

	@Override
	public Lineage computeLineage()
	{
		return LineageNode.of(this, leftOperand.getLineage(), rightOperand.getLineage());
	}
}
