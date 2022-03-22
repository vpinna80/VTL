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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class PeriodIndicatorTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain> DURATION_MEASURE = new DataStructureComponentImpl<>(
			STRINGDS.getVarName(), Measure.class, STRINGDS);

	private final Transformation operand;
	private transient String componentName = null;

	public PeriodIndicatorTransformation(Transformation operand)
	{
		this.operand = operand;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue value;
		if (operand == null)
			value = session.resolve(componentName);
		else
			value = operand.eval(session);

		if (value instanceof ScalarValue)
			return StringValue.of(((TimePeriodValue<?>) value).getPeriodIndicator());
		else
		{
			DataSet ds = (DataSet) value;
			
			DataSetMetadata metadata = new DataStructureBuilder(ds.getComponents(Identifier.class))
					.addComponent(DURATION_MEASURE)
					.build();
			
			DataStructureComponent<?, ? extends TimeDomainSubset<?, ?>, TimeDomain> component;
			if (operand == null)
				component = ds.getComponents(Identifier.class, TIMEDS).iterator().next();
			else
				component = ds.getComponents(Measure.class, TIMEDS).iterator().next();

			return ds.mapKeepingKeys(metadata, dp -> LineageNode.of(this, dp.getLineage()), dp -> singletonMap(DURATION_MEASURE,
							StringValue.of(((TimePeriodValue<?>) dp.get(component)).getPeriodIndicator())));
		}
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata value;
		if (operand == null)
			value = session.getMetadata(THIS);
		else
			value = operand.getMetadata(session);

		if (value instanceof ScalarValueMetadata)
		{
			ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) value).getDomain();
			if (!TIME.isAssignableFrom(domain))
				throw new VTLIncompatibleTypesException("period_indicator", TIME, domain);
			else
				return STRING;
		}
		else
		{
			DataSetMetadata ds = (DataSetMetadata) value;

			if (operand == null)
			{
				Set<DataStructureComponent<Identifier, ?, ?>> timeIDs = ds.getComponents(Identifier.class).stream()
						.map(c -> c.asRole(Identifier.class))
						.filter(c -> TIMEDS.isAssignableFrom(c.getDomain()))
						.collect(toSet());
				if (timeIDs.size() != 1)
					throw new VTLSingletonComponentRequiredException(Identifier.class, timeIDs);
				
				componentName = timeIDs.iterator().next().getName();
			}
			else
			{
				Set<DataStructureComponent<Measure, ?, ?>> components = ds.getComponents(Measure.class);
				if (components.size() != 1)
					throw new VTLSingletonComponentRequiredException(Measure.class, components);

				DataStructureComponent<Measure, ?, ?> anyDomainComponent = components.iterator().next();
				if (!TIME.isAssignableFrom(anyDomainComponent.getDomain()))
					throw new VTLIncompatibleTypesException("period_indicator", TIMEDS, anyDomainComponent);
				
				componentName = anyDomainComponent.asDomain(TIMEDS).getName();
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
	
	@Override
	public Lineage computeLineage()
	{
		return LineageNode.of(this, operand.getLineage());
	}
}
