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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.Collections.singletonMap;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class MatchTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> BOOL_MEASURE = new DataStructureComponentImpl<>(BOOLEAN.getDomain().getVarName(), Measure.class, BOOLEANDS);

	public MatchTransformation(Transformation operand, Transformation pattern)
	{
		super(operand, pattern);
	}

	@Override
	protected VTLValue evalTwoScalars(ScalarValue<?, ?, ?, ?> string, ScalarValue<?, ?, ?, ?> pattern)
	{
		if (string instanceof NullValue || pattern instanceof NullValue)
			return NullValue.instance(BOOLEANDS);
		
		return BooleanValue.of(STRINGDS.cast(string).get().toString().matches(STRINGDS.cast(pattern).get().toString()));
	}
	
	@Override
	protected VTLValue evalDatasetWithScalar(boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> patternV)
	{
		DataSetMetadata structure = new DataStructureBuilder(dataset.getMetadata().getComponents(Identifier.class))
				.addComponent(BOOL_MEASURE)
				.build();

		DataStructureComponent<Measure, StringDomainSubset, StringDomain> measure = dataset.getComponents(Measure.class, STRINGDS).iterator().next();
		String pattern = patternV instanceof NullValue ? null : STRINGDS.cast(patternV).get().toString();
		
		return dataset.mapKeepingKeys(structure, dp -> singletonMap(BOOL_MEASURE, (pattern == null 
				? BOOLEANDS.cast(NullValue.instance(BOOLEANDS))
				: BooleanValue.of(STRINGDS.cast(dp.get(measure)).get().toString().matches(pattern))))); 
	}
	
	@Override
	protected VTLValue evalTwoDatasets(DataSet left, DataSet right)
	{
		// This should never happen
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?> pattern, ScalarValueMetadata<?> scalar)
	{
		if (!(pattern instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(pattern, ScalarValueMetadata.class);
		else if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?>) pattern).getDomain()))
			throw new VTLIncompatibleTypesException("match_characters: pattern parameter", STRING, ((ScalarValueMetadata<?>) pattern).getDomain());
		else if (!(STRING.isAssignableFrom(scalar.getDomain())))
			throw new VTLIncompatibleTypesException("match_characters", STRING, scalar.getDomain());
		else
			return BOOLEAN;
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?> pattern)
	{
		if (!datasetIsLeftOp)
			throw new VTLInvalidParameterException(pattern, ScalarValueMetadata.class);
		if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?>) pattern).getDomain()))
			throw new VTLIncompatibleTypesException("match_characters: pattern parameter", STRING, ((ScalarValueMetadata<?>) pattern).getDomain());

		final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class);
		if (measures.size() != 1)
			throw new VTLSingletonComponentRequiredException(Measure.class, STRINGDS, measures);
		
		DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
		if (!STRING.isAssignableFrom(measure.getDomain()))
			throw new VTLExpectedComponentException(Measure.class, STRING, measures);
		
		return new DataStructureBuilder(dataset.getComponents(Identifier.class))
				.addComponent(BOOL_MEASURE)
				.build();
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right)
	{
		throw new VTLInvalidParameterException(left, ScalarValueMetadata.class);
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}
	
	@Override
	public String toString()
	{
		return "match_characters(" + getLeftOperand() + ", " + getRightOperand() + ")";
	}
}
