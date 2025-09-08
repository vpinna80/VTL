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
package it.bancaditalia.oss.vtl.impl.transform.number;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLExpectedRoleException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.operators.NumericIntOperator;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class NumericIntTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(NumericIntTransformation.class);
	
	private final NumericIntOperator operator;
	
	public NumericIntTransformation(NumericIntOperator operator, Transformation left, Transformation right)
	{
		super(left, coalesce(right, new ConstantOperand(IntegerValue.of(0L))));

		this.operator = operator;
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalTwoScalars(VTLValueMetadata resultMetadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		return operator.apply(left, INTEGERDS.cast(right));
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata resultMetadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		DataSetStructure dsMeta = (DataSetStructure) resultMetadata;
		Set<VTLAlias> measureNames = dataset.getMetadata().getComponents(Measure.class, NUMBERDS).stream()
				.map(DataSetComponent::getAlias)
				.collect(toSet());
		
		ScalarValue<?, ?, EntireIntegerDomainSubset, IntegerDomain> positions = INTEGERDS.cast(scalar);
		return dataset.mapKeepingKeys(dsMeta, lineageEnricher(this), dp -> { 
				Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>(dp.getValues(Attribute.class));
				for (VTLAlias name: measureNames)
				{
					DataSetComponent<Measure, ?, ?> comp = dsMeta.getComponent(name)
							.orElseThrow(() -> new VTLMissingComponentsException(dp.keySet(), name)).asRole(Measure.class);
					result.put(comp, operator.apply(dp.get(comp), positions));
				}
				return result;
			});
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata resultMetadata, DataSet left, DataSet right)
	{
		throw new UnsupportedOperationException(operator + "with two datasets");
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		ValueDomain domainLeft = left.getDomain();
		ValueDomain domainRight = right.getDomain();
		
		if (!NUMBERDS.isAssignableFrom(domainLeft))
			throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, domainLeft);
		if (!(domainRight instanceof IntegerDomain))
			throw new VTLIncompatibleTypesException(operator.toString(), domainRight, INTEGERDS);

		return left;
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetStructure dataset, ScalarValueMetadata<?, ?> scalar)
	{
		if (!(scalar.getDomain() instanceof IntegerDomain))
			throw new VTLIncompatibleTypesException(operator.toString(), scalar.getDomain(), INTEGERDS);
		
		Set<DataSetComponent<Measure, ?, ?>> measures = dataset.getMeasures();
		if (measures.size() == 0)
			throw new VTLExpectedRoleException(Measure.class, dataset);
		else
		{
			for (DataSetComponent<Measure, ?, ?> measure: measures)
				if (!NUMBERDS.isAssignableFrom(measure.getDomain()))
					throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), measure, NUMBERDS);
			
			return new DataSetStructureBuilder(dataset).removeComponents(dataset.getComponents(Attribute.class)).build();
		}
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(TransformationScheme scheme, DataSetStructure left, DataSetStructure right)
	{
		throw new VTLInvalidParameterException(right, ScalarValueMetadata.class);
	}
	
	@Override
	public String toString()
	{
		return operator + "(" + getLeftOperand().toString() + ", " + getRightOperand() + ")";
	}
}
