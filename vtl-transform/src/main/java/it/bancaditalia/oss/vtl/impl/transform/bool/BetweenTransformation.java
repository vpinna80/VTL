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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static java.util.Collections.singletonMap;

import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
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
	private static final DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOOL_VAR = BOOLEANDS.getDefaultVariable().as(Measure.class);
	
	private final ScalarValue<?, ?, ?, ?> from;
	private final ScalarValue<?, ?, ?, ?> to;

	private final transient ValueDomainSubset<?, ?> domain;

	public BetweenTransformation(Transformation operand, Transformation fromT, Transformation toT)
	{
		super(operand);
		
		if (fromT instanceof ConstantOperand && toT instanceof ConstantOperand)
		{
			this.from = ((ConstantOperand) fromT).eval(null);
			this.to = ((ConstantOperand) toT).eval(null);
		}
		else
			throw new UnsupportedOperationException("Non-constant range parameters in between expression are not supported");
		
		if (!from.getDomain().isAssignableFrom(to.getDomain()) || !to.getDomain().isAssignableFrom(from.getDomain()))
			throw new VTLIncompatibleTypesException("between", from, to);
		if (from.isNull() || to.isNull())
			throw new VTLException("Between: Null constant not allowed.");
		this.domain = from.getDomain(); 
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata op = operand.getMetadata(scheme);

		if (op instanceof DataSetMetadata)
		{
			DataSetMetadata ds = (DataSetMetadata) op;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = ds.getMeasures();

			if (measures.size() != 1)
				throw new UnsupportedOperationException("Expected single measure but found: " + measures);

			DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();

			if (!measure.getVariable().getDomain().isAssignableFrom(domain))
				throw new VTLIncompatibleTypesException("between", measure, domain);

			return new DataStructureBuilder()
					.addComponents(ds.getIDs())
					.addComponent(BOOL_VAR)
					.build();
		}
		else
			return BOOLEAN;
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return scalar.isNull() ? NullValue.instance(BOOLEANDS) : BooleanValue.of(scalar.compareTo(from) >= 0 && scalar.compareTo(to) <= 0);
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		return dataset.mapKeepingKeys((DataSetMetadata) metadata, lineage -> LineageNode.of(this, lineage), dp -> singletonMap(BOOL_VAR, evalOnScalar(repo, dp.get(measure), metadata)));
	}
	
	@Override
	public String toString()
	{
		return "between(" + operand + ", " + from + ", " + to + ")";
	}
}
