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
package it.bancaditalia.oss.vtl.impl.transform.ops;

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.getDefaultMeasure;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.Collections.singletonMap;

import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CastTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias targetAlias; 
	private final String mask;

	public CastTransformation(Transformation operand, Domains targetDomain, String mask)
	{
		super(operand);

		this.targetAlias = targetDomain.getDomain().getAlias();
		this.mask = mask != null ? mask.substring(1, mask.length() - 1) : "";
	}

	public CastTransformation(Transformation operand, VTLAlias targetAlias, String mask)
	{
		super(operand);
		
		this.targetAlias = targetAlias;
		this.mask = mask != null ? mask.substring(1, mask.length() - 1) : "";
	}

	@Override
	protected VTLValue evalOnScalar(TransformationScheme scheme, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		ValueDomainSubset<?, ?> targetDomain = scheme.getRepository().getDomain(targetAlias).orElseThrow(() -> new VTLUndefinedObjectException("domain", targetAlias));
		
		return castScalar(scalar, targetDomain);
	}

	@Override
	protected VTLValue evalOnDataset(TransformationScheme scheme, DataSet dataset, VTLValueMetadata resultMetadata)
	{
		ValueDomainSubset<?, ?> targetDomain = scheme.getRepository().getDomain(targetAlias).orElseThrow(() -> new VTLUndefinedObjectException("domain", targetAlias));
		
		DataSetComponent<Measure, ?, ?> oldMeasure = dataset.getMetadata().getMeasures().iterator().next();
		if (targetDomain == oldMeasure.getDomain())
			return dataset;
		
		DataSetComponent<Measure, ?, ?> measure = getDefaultMeasure(targetDomain);
		DataSetStructure structure = new DataSetStructureBuilder(dataset.getMetadata().getIDs())
				.addComponent(measure)
				.build();
		
		return dataset.mapKeepingKeys(structure, lineageEnricher(this), dp -> singletonMap(measure, castScalar(dp.get(oldMeasure), targetDomain)));
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = operand.getMetadata(scheme);
		ValueDomainSubset<?, ?> targetDomain = scheme.getRepository().getDomain(targetAlias).orElseThrow(() -> new VTLUndefinedObjectException("domain", targetAlias));

		ValueDomainSubset<?, ?> domain;
		if (!meta.isDataSet())
			domain = ((ScalarValueMetadata<?, ?>) meta).getDomain();
		else
		{
			DataSetStructure dataset = (DataSetStructure) meta;
			DataSetComponent<? extends Measure, ?, ?> measure = dataset.getSingleton(Measure.class);
			domain = measure.getDomain();
		}

		// All possible casts
		if (domain == targetDomain)
			return ScalarValueMetadata.of(targetDomain);
		else if (domain instanceof StringDomain && targetDomain instanceof StringDomain)
			return ScalarValueMetadata.of(targetDomain);
		else if (domain instanceof StringDomain && targetDomain instanceof TimePeriodDomain)
			return ScalarValueMetadata.of(targetDomain);
		else if (domain instanceof StringDomain && targetDomain instanceof NumberDomain)
			return ScalarValueMetadata.of(targetDomain);
		else if (domain instanceof StringDomain && targetDomain instanceof DateDomain)
			return ScalarValueMetadata.of(targetDomain);
		else if (domain instanceof TimeDomain && targetDomain instanceof StringDomain)
			return ScalarValueMetadata.of(targetDomain);
		else if (domain instanceof NumberDomain && targetDomain instanceof NumberDomain)
			return ScalarValueMetadata.of(targetDomain);
		else if (domain instanceof NumberDomain && targetDomain instanceof StringDomain)
			return ScalarValueMetadata.of(targetDomain);
		else
			throw new UnsupportedOperationException("cast " + domain + " => " + targetDomain + " not implemented ");
	}

	private ScalarValue<?, ?, ?, ?> castScalar(ScalarValue<?, ?, ?, ?> scalar, ValueDomainSubset<?, ?> targetDomain)
	{
		if (scalar.getDomain() == targetDomain)
			return scalar;
		
		if (mask == null || mask.isBlank())
			return targetDomain.cast(scalar);
		else
			throw new UnsupportedOperationException("cast with mask not implemented.");
	}
	
	@Override
	public String toString()
	{
		return "cast(" + operand + ", " + targetAlias + ", \"" + mask + "\")";
	}
}
