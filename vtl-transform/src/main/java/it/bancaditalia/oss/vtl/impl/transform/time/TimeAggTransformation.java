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
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.Collections.singletonMap;

import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
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

	private final Frequency frequency;
	private final PeriodDelimiter delimiter;

	public TimeAggTransformation(Transformation operand, String periodTo, String periodFrom, PeriodDelimiter delimiter)
	{
		super(operand);
		frequency = Frequency.valueOf(periodTo.replaceAll("^\"(.*)\"$", "$1"));
		this.delimiter = Utils.coalesce(delimiter, LAST);
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalOnScalar(TransformationScheme scheme, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		if (scalar.isNull())
			return scalar;
		else if (scalar instanceof TimeValue)
		{
			TimePeriodValue<?> period = frequency.wrap((TimeValue<?, ?, ?, ?>) scalar);
			if (scalar instanceof DateValue)
				return DateValue.of(delimiter == FIRST ? period.get().startDate() : period.get().endDate());
			else if (scalar instanceof TimePeriodValue)
				return period;
			else
				throw new UnsupportedOperationException("time_agg on time values not implemented");
		}
		else
			throw new VTLIncompatibleTypesException("time_agg", TIMEDS, scalar.getDomain());
	}

	@Override
	protected VTLValue evalOnDataset(TransformationScheme scheme, DataSet dataset, VTLValueMetadata resultMetadata)
	{
		DataSetComponent<Measure, ?, ?> measure = ((DataSetStructure) resultMetadata).getComponents(Measure.class).iterator().next();
		
		return dataset.mapKeepingKeys((DataSetStructure) resultMetadata, lineageEnricher(this), 
				dp -> singletonMap(measure, evalOnScalar(scheme, dp.get(measure), null)));
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata value;
		value = operand.getMetadata(session);

		if (!value.isDataSet())
		{
			ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) value).getDomain();
			if (!TIMEDS.isAssignableFrom(domain))
				throw new VTLIncompatibleTypesException("time_agg", TIMEDS, domain);
			
			return value;
		}
		else
		{
			Set<DataSetComponent<Measure, ?, ?>> measures = ((DataSetStructure) value).getMeasures();
				
			if (measures.size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, TIMEDS, measures);
			
			DataSetComponent<Measure, ?, ?> timeMeasure = measures.iterator().next();
			if (!TIMEDS.isAssignableFrom(timeMeasure.getDomain()))
				throw new VTLIncompatibleTypesException("time_agg", TIMEDS, timeMeasure);
			
			return value;
		}
	}
	
	@Override
	public String toString()
	{
		return "time_agg(\"" + frequency + "\", " + operand + ")";
	}
}
