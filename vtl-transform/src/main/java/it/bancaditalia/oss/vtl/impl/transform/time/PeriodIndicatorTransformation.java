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

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATION;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;

import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDurationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class PeriodIndicatorTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, EntireDurationDomainSubset, DurationDomain> DURATION_MEASURE = DURATIONDS.getDefaultVariable().as(Measure.class);
	
	private final Transformation operand;

	public PeriodIndicatorTransformation(Transformation operand)
	{
		this.operand = operand;
	}
	
	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		VTLValue value = operand != null ? operand.eval(scheme) : ((DatapointScope) scheme).getTimeIdValue();

		if (!value.isDataSet())
			return evalScalar((ScalarValue<?, ?, ? ,?>) value);
		else 
		{
			DataSet dataset = (DataSet) value;
			
			DataStructureComponent<?, ?, ?> component = dataset.getMetadata().getIDs().stream()
					.filter(c -> TIMEDS.isAssignableFrom(c.getVariable().getDomain()))
					.findAny()
					.get();

			return dataset.mapKeepingKeys((DataSetMetadata) getMetadata(scheme), lineage -> LineageNode.of(this, lineage), 
					dp -> singletonMap(DURATION_MEASURE, evalScalar(dp.get(component))));
		}
	}

	private ScalarValue<?, ?, ?, ?> evalScalar(ScalarValue<?, ?, ?, ?> value)
	{
		if (value.isNull())
			return NullValue.instance(TIMEDS);
		else 
			return ((TimeValue<?, ?, ?, ?>) value).getFrequency();
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		if (operand == null)
		{
			Set<DataStructureComponent<Identifier, ?, ?>> ids = ((DataSetMetadata) scheme.getMetadata(THIS)).getIDs();
			
			Set<DataStructureComponent<Identifier, ?, ?>> timeIDs = ids.stream()
					.map(c -> c.asRole(Identifier.class))
					.filter(c -> TIMEDS.isAssignableFrom(c.getVariable().getDomain()))
					.collect(toSet());
			
			if (timeIDs.size() != 1)
				throw new VTLSingletonComponentRequiredException(Identifier.class, timeIDs);

			return new DataStructureBuilder(ids)
					.addComponent(DURATION_MEASURE)
					.build();
		}
		else
		{
			VTLValueMetadata metadata = operand.getMetadata(scheme);
			
			if (!metadata.isDataSet())
			{
				ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) metadata).getDomain();
				if (!TIMEDS.isAssignableFrom(domain))
					throw new VTLIncompatibleTypesException("period_indicator", TIMEDS, domain);
				else
					return DURATION;
			}
			else
			{
				Set<DataStructureComponent<Identifier, ?, ?>> ids = ((DataSetMetadata) metadata).getIDs();
				
				Set<DataStructureComponent<Identifier, ?, ?>> timeIDs = ids.stream()
						.map(c -> c.asRole(Identifier.class))
						.filter(c -> TIMEDS.isAssignableFrom(c.getVariable().getDomain()))
						.collect(toSet());
				
				if (timeIDs.size() != 1)
					throw new VTLSingletonComponentRequiredException(Identifier.class, timeIDs);
			
				return new DataStructureBuilder(ids)
						.addComponent(DURATION_MEASURE)
						.build();
			}
		}
	}

	@Override
	public boolean isTerminal()
	{
		return operand == null;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return operand == null ? emptySet() : operand.getTerminals();
	}
	
	@Override
	public String toString()
	{
		return "period_indicator(" + coalesce(operand, "") + ")"; 
	}
}
