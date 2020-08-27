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

import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanUnaryTransformation.BooleanUnaryOperator.NOT;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class BooleanUnaryTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public enum BooleanUnaryOperator //implements Function<ScalarValue<?, BooleanDomainSubset, BooleanDomain>, ScalarValue<?, BooleanDomainSubset, BooleanDomain>>
	{
		NOT(a -> !a), CHECK(a -> a);

		private final Predicate<Boolean> booleanOp;

		private BooleanUnaryOperator(Predicate<Boolean> booleanOp)
		{
			this.booleanOp = booleanOp;
		}

		public ScalarValue<?, BooleanDomainSubset, BooleanDomain> apply(ScalarValue<?, BooleanDomainSubset, BooleanDomain> booleanValue)
		{
			return booleanValue instanceof NullValue ? NullValue.instance(BOOLEANDS)
					: BooleanValue.of(booleanOp.test((Boolean) booleanValue.get()));
		}
	}

	private final static BooleanUnaryOperator function = NOT;

	public BooleanUnaryTransformation(Transformation operand)
	{
		super(operand);
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		return function.apply(BOOLEANDS.cast(scalar));
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		Set<DataStructureComponent<Measure, ?, ?>> components = dataset.getComponents(Measure.class);

		return dataset.mapKeepingKeys(dataset.getMetadata(), dp -> {
			Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?>> map = new HashMap<>(dp.getValues(components, Measure.class));
			map.replaceAll((c, v) -> function.apply(BOOLEANDS.cast(v)));
			return map;
		});
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);

		if (meta instanceof VTLScalarValueMetadata)
			if (((VTLScalarValueMetadata<?>) meta).getDomain() instanceof BooleanDomainSubset)
				return BOOLEAN;
			else
				throw new VTLIncompatibleTypesException(function.toString(), BOOLEANDS, ((VTLScalarValueMetadata<?>) meta).getDomain());
		else
		{
			VTLDataSetMetadata dataset = (VTLDataSetMetadata) meta;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class);
			if (dataset.getComponents(Measure.class).size() == 0)
				throw new UnsupportedOperationException("Expected at least 1 measure but found none.");

			measures.removeAll(dataset.getComponents(Measure.class, BOOLEANDS));
			if (measures.size() > 0)
				throw new UnsupportedOperationException("Expected only numeric measures but found: " + measures);

			return dataset;
		}
	}
}
