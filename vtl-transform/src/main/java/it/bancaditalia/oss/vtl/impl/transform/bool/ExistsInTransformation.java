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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.transform.bool.ExistsInTransformation.ExistsInMode.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.bool.ExistsInTransformation.ExistsInMode.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleMeasuresException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
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
	private VTLDataSetMetadata metadata;

	public ExistsInTransformation(ExistsInMode mode, Transformation left, Transformation right)
	{
		super(left, right);
		
		this.mode = mode == null ? ALL : mode;
	}

	@Override
	protected VTLValue evalTwoScalars(ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalDatasetWithScalar(boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?> scalar)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalTwoDatasets(DataSet left, DataSet right)
	{
		DataStructureComponent<? extends Measure, ?, ?> leftMeasure = left.getComponents(Measure.class).iterator().next(),
				rightMeasure = right.getComponents(Measure.class).iterator().next();
		DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> boolMeasure = metadata.getComponent("bool_var", Measure.class, BOOLEANDS); 
		
		Set<? extends ScalarValue<?, ?, ?>> values = right.stream().map(dp -> dp.get(rightMeasure)).collect(toSet());
		Predicate<DataPoint> filter;
		if (mode == ALL)
			filter = dp -> true;
		else if (mode == TRUE) 
			filter = dp -> values.contains(dp.get(leftMeasure));
		else
			filter = dp -> !values.contains(dp.get(leftMeasure));
		
		Function<DataPoint, Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>>> mapper;
		if (mode == ALL)
			mapper = dp -> singletonMap(boolMeasure, BooleanValue.of(values.contains(dp.get(leftMeasure))));
		else
			mapper = dp -> { 
					HashMap<DataStructureComponent<? extends NonIdentifier, ?, ?>, ScalarValue<?, ?, ?>> map = new HashMap<>(dp.getValues(NonIdentifier.class));
					map.put(boolMeasure, BooleanValue.of(values.contains(dp.get(leftMeasure))));
					map.remove(leftMeasure);
					return map;
				};
			
		return left.filter(filter).mapKeepingKeys(metadata, mapper);
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata left = leftOperand.getMetadata(session), right = rightOperand.getMetadata(session);
		
		if (!(left instanceof VTLDataSetMetadata))
			throw new VTLInvalidParameterException(left, VTLDataSetMetadata.class);
		if (!(right instanceof VTLDataSetMetadata))
			throw new VTLInvalidParameterException(right, VTLDataSetMetadata.class);
		
		Set<? extends DataStructureComponent<? extends Measure, ?, ?>> leftMeasures = ((VTLDataSetMetadata) left).getComponents(Measure.class),
				rightMeasures = ((VTLDataSetMetadata) right).getComponents(Measure.class);
		
		if (leftMeasures.size() != 1)
			throw new VTLSingletonComponentRequiredException(Measure.class, leftMeasures);
		if (rightMeasures.size() != 1)
			throw new VTLSingletonComponentRequiredException(Measure.class, rightMeasures);

		DataStructureComponent<? extends Measure, ?, ?> leftMeasure = leftMeasures.iterator().next(),
				rightMeasure = rightMeasures.iterator().next();
		
		if (!leftMeasure.getDomain().isAssignableFrom(rightMeasure.getDomain()) && !rightMeasure.getDomain().isAssignableFrom(leftMeasure.getDomain()))
			throw new VTLIncompatibleMeasuresException("EXISTS_IN", leftMeasure, rightMeasure);
		
		DataStructureBuilder builder = new DataStructureBuilder((VTLDataSetMetadata) left)
				.addComponent(new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS));
		
		if (mode != ALL)
			builder.removeComponent(leftMeasure);
		
		return metadata = builder.build();
	}

	@Override
	public String toString()
	{
		return "exists_in(" + leftOperand + ", " + rightOperand + ", " + mode + ")";
	}
}
