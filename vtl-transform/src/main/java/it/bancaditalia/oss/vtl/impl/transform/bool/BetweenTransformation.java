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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.Collections.singletonMap;

import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class BetweenTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	
	private final Transformation from;
	private final Transformation to;

	public BetweenTransformation(Transformation operand, Transformation from, Transformation to)
	{
		super(operand);

		this.from = from;
		this.to = to;
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata, TransformationScheme scheme)
	{
		ScalarValue<?, ?, ?, ?> fromValue = (ScalarValue<?, ?, ?, ?>) from.eval(scheme);
		ScalarValue<?, ?, ?, ?> toValue = (ScalarValue<?, ?, ?, ?>) to.eval(scheme);
		
		return test(scalar, fromValue, toValue);
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata, TransformationScheme scheme)
	{
		ScalarValue<?, ?, ?, ?> fromValue = (ScalarValue<?, ?, ?, ?>) from.eval(scheme);
		ScalarValue<?, ?, ?, ?> toValue = (ScalarValue<?, ?, ?, ?>) to.eval(scheme);
		
		DataSetComponent<? extends Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		return dataset.mapKeepingKeys((DataSetStructure) metadata, lineageEnricher(this), 
				dp -> singletonMap(BOOL_VAR, test(dp.get(measure), fromValue, toValue)));
	}

	private static ScalarValue<?, ?, EntireBooleanDomainSubset, BooleanDomain> test(ScalarValue<?, ?, ?, ?> scalar,
			ScalarValue<?, ?, ?, ?> fromValue, ScalarValue<?, ?, ?, ?> toValue) 
	{
		if (scalar.isNull()) 
			return NullValue.instance(BOOLEANDS);
		else
			return BooleanValue.of(scalar.compareTo(fromValue) >= 0 && scalar.compareTo(toValue) <= 0);
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata op = operand.getMetadata(scheme);
		VTLValueMetadata fromOp = from.getMetadata(scheme);
		VTLValueMetadata toOp = to.getMetadata(scheme);

		if (op.isDataSet())
		{
			DataSetStructure ds = (DataSetStructure) op;

			Set<? extends DataSetComponent<? extends Measure, ?, ?>> measures = ds.getMeasures();

			if (measures.size() != 1)
				throw new UnsupportedOperationException("Expected single measure but found: " + measures);

			DataSetComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
			
			if (!(fromOp instanceof ScalarValueMetadata))
				throw new VTLInvalidParameterException(fromOp, ScalarValueMetadata.class);
			if (!(toOp instanceof ScalarValueMetadata))
				throw new VTLInvalidParameterException(toOp, ScalarValueMetadata.class);
			
			ValueDomainSubset<?, ?> fromDomain = ((ScalarValueMetadata<?, ?>) fromOp).getDomain();
			ValueDomainSubset<?, ?> toDomain = ((ScalarValueMetadata<?, ?>) toOp).getDomain();

			if (!measure.getDomain().isAssignableFrom(fromDomain))
				throw new VTLIncompatibleTypesException("between", measure, fromDomain);
			if (!measure.getDomain().isAssignableFrom(toDomain))
				throw new VTLIncompatibleTypesException("between", measure, toDomain);

			return new DataSetStructureBuilder()
					.addComponents(ds.getIDs())
					.addComponent(BOOL_VAR)
					.build();
		}
		else
			return BOOLEAN;
	}
	
	@Override
	public String toString()
	{
		return "between(" + operand + ", " + from + ", " + to + ")";
	}
}
