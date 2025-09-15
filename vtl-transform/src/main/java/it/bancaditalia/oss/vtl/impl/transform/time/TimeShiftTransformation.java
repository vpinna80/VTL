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

import static it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl.DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.DURATION_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.util.SortClause;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowClauseImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeWithFreq;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDurationDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class TimeShiftTransformation extends TimeSeriesTransformation
{
	private static final long serialVersionUID = 1L;

	private final long amount;

	public TimeShiftTransformation(Transformation operand, ScalarValue<?, ?, ?, ?> amount)
	{
		super(operand);
		
		this.amount = ((IntegerValue<?, ?>) amount).get();
	}

	@Override
	protected VTLValue evalOnDataset(TransformationScheme scheme, DataSet dataset, VTLValueMetadata resultMetadata)
	{
		DataSetStructure dsMeta = dataset.getMetadata();
		DataSetComponent<Identifier, ?, ?> timeID = dsMeta.getComponents(Identifier.class, TIMEDS).iterator().next();
		
		SerCollector<TimeValue<?, ?, ?, ?>, ?, DurationValue> timesToFreq = SerCollector.of(TimeWithFreq::new, 
				TimeWithFreq::setTime, TimeWithFreq::combine, TimeWithFreq::getDuration, Set.of());
		DataSetComponent<Identifier, EntireDurationDomainSubset, DurationDomain> freqComp = 
					new DataSetComponentImpl<>(DURATION_VAR.getAlias(), Identifier.class, DURATIONDS);
		
		Set<DataSetComponent<?, ?, ?>> idsNoTimeWithFreq = new HashSet<>(dsMeta.getIDs());
		idsNoTimeWithFreq.remove(timeID);
		idsNoTimeWithFreq.add(freqComp);
		
		DataSetStructure withFreq = new DataSetStructureBuilder(dsMeta).addComponent(freqComp).build();
		dataset = dataset.mapKeepingKeys(withFreq, identity(), dp -> {
			Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>(dp.getValues(NonIdentifier.class));
			map.put(freqComp, ((TimeValue<?, ?, ?, ?>) dp.get(timeID)).getFrequency());
			return map;
		});

		WindowClause clause = new WindowClauseImpl(idsNoTimeWithFreq, List.of(new SortClause(timeID)), DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING);
		dataset = dataset.analytic(lineageEnricher(this), timeID, freqComp, clause, null, timesToFreq, null);

		DataSetStructure structure = (DataSetStructure) resultMetadata;
		SerUnaryOperator<Lineage> enricher = lineageEnricher(this);
		return new FunctionDataSet<>(structure, ds -> ds.stream()
				.map(dp -> new DataPointBuilder(dp)
					.delete(timeID)
					.delete(freqComp)
					.add(timeID, ((TimeValue<?, ?, ?, ?>) dp.get(timeID)).add(((Frequency) dp.get(freqComp).get()).getScaledPeriod((int) amount)))
					.build(enricher.apply(dp.getLineage()), structure)
				), dataset);
	}

	@Override
	protected DataSetStructure checkIsTimeSeriesDataSet(DataSetStructure metadata, TransformationScheme scheme)
	{
		return metadata;
	}
	
	@Override
	public String toString()
	{
		return "timeshift(" + operand + ", " + amount + ")";
	}
}
