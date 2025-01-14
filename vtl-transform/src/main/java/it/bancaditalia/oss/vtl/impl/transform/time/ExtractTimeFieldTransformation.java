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
package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.operators.TimeFieldOperator;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class ExtractTimeFieldTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final TimeFieldOperator field;

	public ExtractTimeFieldTransformation(TimeFieldOperator field, Transformation operand)
	{
		super(operand);
		this.field = field;
	}

	@Override
	public VTLValue evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return IntegerValue.of((long) ((TimeValue<?, ?, ?, ?>) scalar).getEndDate().get().get(field.getField()));
	}

	@Override
	public VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata input = operand.getMetadata(scheme);
		
		if (input instanceof ScalarValueMetadata)
		{
			ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) input).getDomain();
			if (domain instanceof TimeDomainSubset)
				return INTEGER;
			else
				throw new VTLIncompatibleTypesException(field.toString(), TIMEDS, domain);
		}
		else if (scheme instanceof ThisScope)
		{
			DataStructureComponent<Measure, ?, ?> measure = ((DataSetMetadata) input).getMeasures().iterator().next();
			ValueDomainSubset<?, ?> domain = measure.getVariable().getDomain();
			if (domain instanceof TimeDomainSubset)
				return INTEGER;
			else
				throw new VTLIncompatibleTypesException(field.toString(), TIMEDS, domain);
		}
		else
			return computeMetadata(new ThisScope(scheme.getRepository(), (DataSetMetadata) input));
	}
}
