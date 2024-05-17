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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleStructuresException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class ExistsInTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;

	public enum ExistsInMode 
	{
		ALL, TRUE, FALSE;
	}
	
	private final ExistsInMode mode;

	public ExistsInTransformation(ExistsInMode mode, Transformation left, Transformation right)
	{
		super(left, right);
		
		this.mode = mode == null ? ALL : mode;
	}

	@Override
	protected VTLValue evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected DataSet evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		DataStructureComponent<Measure, ?, ?> boolMeasure = BOOLEANDS.getDefaultVariable().as(Measure.class);
		
		Set<DataStructureComponent<Identifier, ?, ?>> commonIDs = new HashSet<>(left.getMetadata().getIDs());
		commonIDs.retainAll(right.getMetadata().getIDs());
		
		Set<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> rightIDValues = right.stream().map(dp -> dp.getValues(commonIDs)).collect(toSet());
			
		DataSet unfiltered = left.mapKeepingKeys((DataSetMetadata) metadata, dp -> LineageNode.of(this, dp.getLineage()), 
				dp -> singletonMap(boolMeasure, BooleanValue.of(rightIDValues.contains(dp.getValues(commonIDs)))));
		
		switch (mode)
		{
			case ALL: return unfiltered;
			case FALSE: return unfiltered.filter(dp -> (BooleanValue<?>) dp.get(boolMeasure) == BooleanValue.FALSE, identity());
			case TRUE: return unfiltered.filter(dp -> (BooleanValue<?>) dp.get(boolMeasure) == BooleanValue.TRUE, identity());
			default: throw new IllegalStateException("unreachable");
		}
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		throw new VTLInvalidParameterException(left, DataSetMetadata.class);	
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?, ?> scalar)
	{
		throw new VTLInvalidParameterException(scalar, DataSetMetadata.class);
	}
	
	@Override
	protected DataSetMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> leftIDs = left.getIDs();
		Set<DataStructureComponent<Identifier, ?, ?>> rightIDs = right.getIDs();
		
		if (!rightIDs.containsAll(leftIDs) && !leftIDs.containsAll(rightIDs))
			throw new VTLIncompatibleStructuresException("exists_in", List.of(leftIDs, rightIDs));
		
		return new DataStructureBuilder(leftIDs)
				.addComponent(BOOLEANDS.getDefaultVariable().as(Measure.class))
				.build();
	}

	@Override
	public String toString()
	{
		return "exists_in(" + getLeftOperand() + ", " + getRightOperand() + ", " + mode + ")";
	}
}
