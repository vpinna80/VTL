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

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATION;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DurationDomain;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class PeriodIndicatorTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;

	private static final DataStructureComponent<Measure, DurationDomainSubset, DurationDomain> DURATION_MEASURE = new DataStructureComponentImpl<>(
			DURATIONDS.getVarName(), Measure.class, DURATIONDS);

	private DataStructureComponent<?, TimeDomainSubset<TimeDomain>, TimeDomain> component = null;

	private final Transformation operand;

	public PeriodIndicatorTransformation(Transformation operand)
	{
		this.operand = operand;
		
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue value;
		if (operand == null)
			value = session.resolve(component.getName());
		else
			value = operand.eval(session);

		if (value instanceof ScalarValue)
			return ((TimeValue<?, ?, ?>) value).getPeriodIndicator();
		else
		{
			DataSet ds = (DataSet) value;
			
			VTLDataSetMetadata metadata = new DataStructureBuilder(ds.getComponents(Identifier.class))
					.addComponent(DURATION_MEASURE)
					.build();
			
			DataStructureComponent<?, TimeDomainSubset<TimeDomain>, TimeDomain> component;
			if (operand == null)
				component = ds.getComponents(Identifier.class, TIMEDS).iterator().next();
			else
				component = ds.getComponents(Measure.class, TIMEDS).iterator().next();

			return ds.mapKeepingKeys(metadata, dp -> singletonMap(DURATION_MEASURE, ((TimeValue<?, ?, ?>) dp.get(component)).getPeriodIndicator()));
		}
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata value;
		if (operand == null)
			value = session.getMetadata(THIS);
		else
			value = operand.getMetadata(session);

		if (value instanceof VTLScalarValueMetadata)
		{
			ValueDomainSubset<?> domain = ((VTLScalarValueMetadata<?>) value).getDomain();
			if (!TIME.isAssignableFrom(domain))
				throw new VTLIncompatibleTypesException("period_indicator", TIME, domain);
			else
				return DURATION;
		}
		else
		{
			VTLDataSetMetadata ds = (VTLDataSetMetadata) value;

			if (operand == null)
			{
				Set<DataStructureComponent<Identifier, TimeDomainSubset<TimeDomain>, TimeDomain>> timeIDs = ds.getComponents(Identifier.class, TIMEDS);
				if (timeIDs.size() != 1)
					throw new VTLSingletonComponentRequiredException(Identifier.class, timeIDs);
				
				component = timeIDs.iterator().next();
			}
			else
			{
				Set<DataStructureComponent<Measure, ?, ?>> components = ds.getComponents(Measure.class);
				if (components.size() != 1)
					throw new VTLSingletonComponentRequiredException(Measure.class, components);

				DataStructureComponent<Measure, ?, ?> anyDomainComponent = components.iterator().next();
				if (!TIME.isAssignableFrom(anyDomainComponent.getDomain()))
					throw new VTLIncompatibleTypesException("period_indicator", TIMEDS, anyDomainComponent);
				
				component = anyDomainComponent.as(TIMEDS);
			}

			return new DataStructureBuilder(ds.getComponents(Identifier.class))
					.addComponent(DURATION_MEASURE)
					.build();
		}
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return operand != null ? operand.getTerminals() : emptySet();
	}
}
