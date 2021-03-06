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
import static java.util.Collections.singletonMap;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class StrlenTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	
	public StrlenTransformation(Transformation operand)
	{
		super(operand);
	}

	@Override
	protected ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return scalar instanceof NullValue ? NullValue.instance(INTEGERDS) : IntegerValue.of((long) ((StringValue<?, ?>) scalar).get().length());
	}

	@Override
	protected DataSet evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		DataStructureComponent<Measure, ? extends IntegerDomainSubset<?>, IntegerDomain> resultMeasure = new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS);
		DataStructureComponent<Measure, ? extends StringDomainSubset<?>, StringDomain> originalMeasure = dataset.getComponents(Measure.class, STRINGDS).iterator().next();
		
		DataSetMetadata structure = new DataStructureBuilder(dataset.getComponents(Identifier.class))
				.addComponent(new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS))
				.build();
		
		return dataset.mapKeepingKeys(structure, dp -> LineageNode.of(this, dp.getLineage()), dp -> singletonMap(resultMeasure, evalOnScalar(dp.get(originalMeasure), metadata)));
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata op = operand.getMetadata(session);
		if (op instanceof ScalarValueMetadata && ((ScalarValueMetadata<?, ?>) op).getDomain() instanceof StringDomainSubset)
			return INTEGER;
		else 
		{
			DataSetMetadata ds = (DataSetMetadata) op;
			if (ds.getComponents(Measure.class).size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, STRINGDS, ds.getComponents(Measure.class));
			if (ds.getComponents(Measure.class, STRINGDS).size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, STRINGDS, ds.getComponents(Measure.class));
			
			return new DataStructureBuilder(ds.getComponents(Identifier.class))
				.addComponent(new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS))
				.build();
		}
	}
	
	@Override
	public String toString() 
	{
		return "length(" + operand + ")";
	}

	@Override
	public int hashCode()
	{
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof StrlenTransformation)) return false;
		return true;
	}
}
