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

import static it.bancaditalia.oss.vtl.impl.transform.time.TimeAggTransformation.PeriodDelimiter.FIRST;
import static it.bancaditalia.oss.vtl.impl.transform.time.TimeAggTransformation.PeriodDelimiter.LAST;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static java.util.stream.Collectors.toSet;

import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue.Durations;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class TimeAggTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	
	public enum PeriodDelimiter
	{
		FIRST, LAST
	}

	private final DataStructureComponent<Measure, ?, ?> periodComponent;
	private final Durations frequency;
	private final PeriodDelimiter delimiter;

	public TimeAggTransformation(Transformation operand, String periodTo, String periodFrom, PeriodDelimiter delimiter)
	{
		super(operand);
		frequency = Durations.valueOf(periodTo.replaceAll("^\"(.*)\"$", "$1"));
		TimePeriodDomainSubset<?> periodDomain = frequency.getRelatedTimePeriodDomain();
		periodComponent = DataStructureComponentImpl.of(Measure.class, periodDomain).asRole(Measure.class);
		this.delimiter = Utils.coalesce(delimiter, LAST);
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		if (scalar instanceof NullValue)
			return scalar;
		else if (scalar instanceof DateValue)
		{
			TimePeriodValue<?> period = ((DateValue<?>) scalar).wrap(frequency);
			return delimiter == FIRST ? period.get().startDate() : period.get().endDate();
		}
		else if (scalar instanceof TimePeriodValue)
			return ((TimeValue<?, ?, ?, ?>) scalar).wrap(frequency);
		else
			throw new UnsupportedOperationException("time_agg on time values not implemented");
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		DataStructureComponent<Measure, ?, ?> timeMeasure = dataset.getMetadata().getComponents(Measure.class, TIMEDS).iterator().next();
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
				return periodComponent.getVariable();
		}
		else
		{
			DataSetMetadata ds = (DataSetMetadata) value;

			Set<DataStructureComponent<Measure, ?, ?>> timeMeasures = ds.getMeasures().stream()
					.map(c -> c.asRole(Measure.class))
					.filter(c -> TIMEDS.isAssignableFrom(c.getDomain()))
					.collect(toSet());
				
			if (timeMeasures.size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, TIMEDS, ds);
			
			return new DataStructureBuilder(ds.getIDs())
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
