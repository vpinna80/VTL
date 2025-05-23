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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.Collections.singletonMap;

import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class StrlenTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, ? extends IntegerDomainSubset<?>, IntegerDomain> LEN_MEASURE = INTEGERDS.getDefaultVariable().as(Measure.class);
	
	public StrlenTransformation(Transformation operand)
	{
		super(operand);
	}

	private static ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> staticEvalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return scalar.isNull() ? NullValue.instance(INTEGERDS) : IntegerValue.of((long) ((StringValue<?, ?>) scalar).get().length());
	}

	@Override
	protected ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata, TransformationScheme scheme)
	{
		return staticEvalOnScalar(repo, scalar, metadata);
	}

	@Override
	protected DataSet evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata, TransformationScheme scheme)
	{
		DataStructureComponent<Measure, ?, ?> originalMeasure = dataset.getMetadata().getComponents(Measure.class, STRINGDS).iterator().next();
		
		DataSetMetadata structure = new DataStructureBuilder(dataset.getMetadata().getIDs())
				.addComponent(LEN_MEASURE)
				.build();
		
		return dataset.mapKeepingKeys(structure, lineageEnricher(this), dp -> singletonMap(LEN_MEASURE, staticEvalOnScalar(repo, dp.get(originalMeasure), metadata)));
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata op = operand.getMetadata(session);
		if (!op.isDataSet() && ((ScalarValueMetadata<?, ?>) op).getDomain() instanceof StringDomainSubset)
			return INTEGER;
		else 
		{
			DataSetMetadata ds = (DataSetMetadata) op;
			if (ds.getMeasures().size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, STRINGDS, ds.getMeasures());
			if (ds.getComponents(Measure.class, STRINGDS).size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, STRINGDS, ds.getMeasures());
			
			return new DataStructureBuilder(ds.getIDs())
				.addComponent(LEN_MEASURE)
				.build();
		}
	}
	
	@Override
	public String toString() 
	{
		return "length(" + operand + ")";
	}
}
