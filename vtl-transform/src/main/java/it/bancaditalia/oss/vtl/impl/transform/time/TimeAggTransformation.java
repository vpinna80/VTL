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
package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class TimeAggTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final DataStructureComponentImpl<Measure, TimePeriodDomainSubset, TimePeriodDomain> periodComponent;
	private final DurationDomains frequency;

	public TimeAggTransformation(Transformation operand, String periodTo)
	{
		super(operand);
		frequency = DurationDomains.valueOf(periodTo.replaceAll("^\"(.*)\"$", "$1"));
		TimePeriodDomainSubset periodDomain = frequency.getRelatedTimePeriodDomain();
		periodComponent = new DataStructureComponentImpl<>(periodDomain.getVarName(), Measure.class, periodDomain);
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		return ((TimeValue<?, ?, ?>) scalar).wrap(frequency);
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		DataStructureComponent<Measure, TimeDomainSubset<TimeDomain>, TimeDomain> timeMeasure = dataset.getComponents(Measure.class, TIMEDS).iterator().next();
		DataSetMetadata structure = new DataStructureBuilder(dataset.getMetadata())
				.addComponent(periodComponent)
				.build();
		
		return dataset.mapKeepingKeys(structure, dp -> {
				Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?>> map = dp.getValues(Measure.class);
				map.put(periodComponent, ((TimeValue<?, ?, ?>) dp.get(timeMeasure)).wrap(frequency));
				return map;
		});
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata value;
		value = operand.getMetadata(session);

		if (value instanceof ScalarValueMetadata)
		{
			ValueDomainSubset<?> domain = ((ScalarValueMetadata<?>) value).getDomain();
			if (!TIMEDS.isAssignableFrom(domain))
				throw new VTLIncompatibleTypesException("time_agg", TIMEDS, domain);
			else
				return (ScalarValueMetadata<?>) periodComponent::getDomain;
		}
		else
		{
			DataSetMetadata ds = (DataSetMetadata) value;

//			if (operand == null)
//			{
//				Set<DataStructureComponent<Identifier, TimeDomainSubset<TimeDomain>, TimeDomain>> timeIDs = ds.getComponents(Identifier.class, TIMEDS);
//				if (timeIDs.size() != 1)
//					throw new VTLSingletonComponentRequiredException(Identifier.class, timeIDs);
//			}
//			else
//			{
				Set<DataStructureComponent<Measure, TimeDomainSubset<TimeDomain>, TimeDomain>> timeMeasures = ds.getComponents(Measure.class, TIMEDS);
				if (timeMeasures.size() != 1)
					throw new VTLSingletonComponentRequiredException(Measure.class, TIMEDS, ds);
//			}
			
			return new DataStructureBuilder(ds.getComponents(Identifier.class))
					.addComponent(periodComponent)
					.build();
		}
	}
	
	@Override
	public String toString()
	{
		return "time_agg(\"" + frequency + "\", " + operand + ")";
	}
}
