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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.transform.bool.ExistsInTransformation.ExistsInMode.ALL;
import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.FALSE;
import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleStructuresException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ExistsInTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;

	public enum ExistsInMode 
	{
		ALL, TRUE, FALSE;
	}
	
	private final ExistsInMode mode;

	public ExistsInTransformation(Transformation left, Transformation right, ExistsInMode mode)
	{
		super(left, right);
		
		this.mode = coalesce(mode, ALL);
	}

	@Override
	protected VTLValue evalTwoScalars(VTLValueMetadata resultMetadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata resultMetadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected DataSet evalTwoDatasets(VTLValueMetadata resultMetadata, DataSet left, DataSet right)
	{
		DataSetComponent<Measure, ?, ?> boolMeasure = BOOL_VAR;
		
		Set<DataSetComponent<Identifier, ?, ?>> commonIDs = new HashSet<>(left.getMetadata().getIDs());
		commonIDs.retainAll(right.getMetadata().getIDs());
		
		Set<Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> rightIDValues;
		try (Stream<DataPoint> stream = right.stream())
		{
			rightIDValues = stream.map(dp -> dp.getValues(commonIDs)).collect(toSet());
		}
				
		DataSet unfiltered = left.mapKeepingKeys((DataSetStructure) resultMetadata, lineageEnricher(this), 
				dp -> singletonMap(boolMeasure, BooleanValue.of(rightIDValues.contains(dp.getValues(commonIDs)))));
		
		switch (mode)
		{
			case ALL: return unfiltered;
			case FALSE: return unfiltered.filter(dp -> dp.get(boolMeasure) == FALSE, identity());
			case TRUE: return unfiltered.filter(dp -> dp.get(boolMeasure) == TRUE, identity());
			default: throw new IllegalStateException("unreachable");
		}
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		throw new VTLInvalidParameterException(left, DataSetStructure.class);	
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetStructure dataset, ScalarValueMetadata<?, ?> scalar)
	{
		throw new VTLInvalidParameterException(scalar, DataSetStructure.class);
	}
	
	@Override
	protected DataSetStructure getMetadataTwoDatasets(TransformationScheme scheme, DataSetStructure left, DataSetStructure right)
	{
		Set<DataSetComponent<Identifier, ?, ?>> leftIDs = left.getIDs();
		Set<DataSetComponent<Identifier, ?, ?>> rightIDs = right.getIDs();
		
		if (!rightIDs.containsAll(leftIDs) && !leftIDs.containsAll(rightIDs))
			throw new VTLIncompatibleStructuresException("exists_in", List.of(leftIDs, rightIDs));
		
		return new DataSetStructureBuilder(leftIDs)
				.addComponent(BOOL_VAR)
				.build();
	}

	@Override
	public String toString()
	{
		return "exists_in(" + getLeftOperand() + ", " + getRightOperand() + ", " + mode + ")";
	}
}
