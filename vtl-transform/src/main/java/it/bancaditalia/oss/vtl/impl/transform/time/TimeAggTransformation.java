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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static java.util.stream.Collectors.toSet;

import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue.Durations;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
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
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class TimeAggTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final DataStructureComponent<Measure, ?, ?> periodComponent;
	private final Durations frequency;

	public TimeAggTransformation(Transformation operand, String periodTo)
	{
		super(operand);
		frequency = Durations.valueOf(periodTo.replaceAll("^\"(.*)\"$", "$1"));
		TimePeriodDomainSubset<?> periodDomain = frequency.getRelatedTimePeriodDomain();
		periodComponent = DataStructureComponentImpl.of(periodDomain.getVarName(), Measure.class, periodDomain).as(Measure.class);
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return ((TimeValue<?, ?, ?, ?>) scalar).wrap(frequency);
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		DataStructureComponent<Measure, ? extends TimeDomainSubset<?, ?>, TimeDomain> timeMeasure = dataset.getComponents(Measure.class, TIMEDS).iterator().next();
		DataSetMetadata structure = new DataStructureBuilder(dataset.getMetadata())
				.addComponent(periodComponent)
				.build();
		
		return dataset.mapKeepingKeys(structure, dp -> LineageNode.of(this, dp.getLineage()), dp -> {
				Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> map = dp.getValues(Measure.class);
				map.put(periodComponent, ((TimeValue<?, ?, ?, ?>) dp.get(timeMeasure)).wrap(frequency));
				return map;
		});
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata value;
		value = operand.getMetadata(session);

		if (value instanceof ScalarValueMetadata)
		{
			ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) value).getDomain();
			if (!TIMEDS.isAssignableFrom(domain))
				throw new VTLIncompatibleTypesException("time_agg", TIMEDS, domain);
			else
				return periodComponent.getMetadata();
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
				Set<DataStructureComponent<Measure, ?, ?>> timeMeasures = ds.getComponents(Measure.class).stream()
						.map(c -> c.as(Measure.class))
						.filter(c -> TIMEDS.isAssignableFrom(c.getDomain()))
						.collect(toSet());
				
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

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((frequency == null) ? 0 : frequency.hashCode());
		result = prime * result + ((periodComponent == null) ? 0 : periodComponent.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof TimeAggTransformation)) return false;
		TimeAggTransformation other = (TimeAggTransformation) obj;
		if (frequency != other.frequency) return false;
		if (periodComponent == null)
		{
			if (other.periodComponent != null) return false;
		}
		else if (!periodComponent.equals(other.periodComponent)) return false;
		return true;
	}
}
