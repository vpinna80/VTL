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
package it.bancaditalia.oss.vtl.impl.transform.string;

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.Collections.singletonMap;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class MatchTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;

	public MatchTransformation(Transformation operand, Transformation pattern)
	{
		super(operand, pattern);
	}

	@Override
	protected VTLValue evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> string, ScalarValue<?, ?, ?, ?> pattern)
	{
		if (string.isNull() || pattern.isNull())
			return NullValue.instance(BOOLEANDS);
		
		return BooleanValue.of(STRINGDS.cast(string).get().toString().matches(STRINGDS.cast(pattern).get().toString()));
	}
	
	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> patternV)
	{
		DataSetStructure structure = new DataSetStructureBuilder(dataset.getMetadata().getIDs())
				.addComponent(BOOL_VAR)
				.build();

		DataSetComponent<Measure, ?, ?> measure = dataset.getMetadata().getComponents(Measure.class, STRINGDS).iterator().next();
		String pattern = patternV.isNull() ? null : STRINGDS.cast(patternV).get().toString();
		
		return dataset.mapKeepingKeys(structure, lineageEnricher(this), dp -> singletonMap(BOOL_VAR, (pattern == null 
						? BOOLEANDS.cast(NullValue.instance(BOOLEANDS))
						: BooleanValue.of(STRINGDS.cast(dp.get(measure)).get().toString().matches(pattern))))); 
	}
	
	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		// This should never happen
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> pattern, ScalarValueMetadata<?, ?> scalar)
	{
		if (!(!pattern.isDataSet()))
			throw new VTLInvalidParameterException(pattern, ScalarValueMetadata.class);
		else if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) pattern).getDomain()))
			throw new VTLIncompatibleTypesException("match_characters: pattern parameter", STRINGDS, ((ScalarValueMetadata<?, ?>) pattern).getDomain());
		else if (!(STRING.isAssignableFrom(scalar.getDomain())))
			throw new VTLIncompatibleTypesException("match_characters", STRINGDS, scalar.getDomain());
		else
			return BOOLEAN;
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetStructure dataset, ScalarValueMetadata<?, ?> pattern)
	{
		if (!datasetIsLeftOp)
			throw new VTLInvalidParameterException(pattern, ScalarValueMetadata.class);
		if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) pattern).getDomain()))
			throw new VTLIncompatibleTypesException("match_characters: pattern parameter", STRINGDS, ((ScalarValueMetadata<?, ?>) pattern).getDomain());

		dataset.getSingleton(Measure.class, STRINGDS);
		
		return new DataSetStructureBuilder(dataset.getIDs())
				.addComponent(BOOL_VAR)
				.build();
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(TransformationScheme scheme, DataSetStructure left, DataSetStructure right)
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
