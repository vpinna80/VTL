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

import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanUnaryTransformation.BooleanUnaryOperator.NOT;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class BooleanUnaryTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public enum BooleanUnaryOperator //implements Function<ScalarValue<?, EntireBooleanDomainSubset, BooleanDomain>, ScalarValue<?, EntireBooleanDomainSubset, BooleanDomain>>
	{
		NOT(a -> !a), CHECK(a -> a);

		private final Predicate<Boolean> booleanOp;

		private BooleanUnaryOperator(Predicate<Boolean> booleanOp)
		{
			this.booleanOp = booleanOp;
		}

		public ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> apply(ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> booleanValue)
		{
			return booleanValue.isNull() ? NullValue.instance(BOOLEANDS)
					: BooleanValue.of(booleanOp.test((Boolean) booleanValue.get()));
		}
	}

	private final static BooleanUnaryOperator function = NOT;

	public BooleanUnaryTransformation(Transformation operand)
	{
		super(operand);
	}

	@Override
	protected VTLValue evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return function.apply(BOOLEANDS.cast(scalar));
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		Set<DataStructureComponent<Measure, ?, ?>> components = dataset.getMetadata().getMeasures();

		return dataset.mapKeepingKeys(dataset.getMetadata(), lineage -> LineageNode.of(this, lineage), dp -> {
			Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(components, Measure.class));
			map.replaceAll((c, v) -> function.apply(BOOLEANDS.cast(v)));
			return map;
		});
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);

		if (!meta.isDataSet())
			if (((ScalarValueMetadata<?, ?>) meta).getDomain() instanceof BooleanDomainSubset)
				return BOOLEAN;
			else
				throw new VTLIncompatibleTypesException(function.toString(), BOOLEANDS, ((ScalarValueMetadata<?, ?>) meta).getDomain());
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getMeasures();
			if (dataset.getMeasures().size() == 0)
				throw new UnsupportedOperationException("Expected at least 1 measure but found none.");

			measures.removeAll(dataset.getComponents(Measure.class, BOOLEANDS));
			if (measures.size() > 0)
				throw new UnsupportedOperationException("Expected only numeric measures but found: " + measures);

			return dataset;
		}
	}
}
