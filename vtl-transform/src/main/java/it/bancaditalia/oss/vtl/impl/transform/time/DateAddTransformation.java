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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DateAddTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;

	private final Transformation operand;
	private final Transformation shift;
	private final Transformation duration;
	
	public DateAddTransformation(Transformation operand, Transformation shift, Transformation duration)
	{
		this.operand = operand;
		this.shift = shift;
		this.duration = duration;
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		long i = ((IntegerValue<?, ?>) shift.eval(scheme)).get();
		Frequency freq = Frequency.valueOf(((StringValue<?, ?>) duration.eval(scheme)).get());
		VTLValue op = operand.eval(scheme);
		
		if (op instanceof ScalarValue)
			return evalOnScalar(i, freq, (ScalarValue<?, ?, ?, ?>) op);

		DataSet dataset = (DataSet) op;
		return dataset.mapKeepingKeys(dataset.getMetadata(), lineageEnricher(this), dp -> {
				Map<DataStructureComponent<NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> map = dp.getValues(NonIdentifier.class);
				for (Entry<DataStructureComponent<NonIdentifier, ?, ?>, ScalarValue<?, ?, ?, ?>> entry: map.entrySet())
					if (entry.getKey().is(Measure.class))
						map.put(entry.getKey(), evalOnScalar(i, freq, entry.getValue()));
	
				return map;
			});
	}

	private ScalarValue<?, ?, ?, ?> evalOnScalar(long i, Frequency freq, ScalarValue<?, ?, ?, ?> op)
	{
		if (op.isNull())
			return op;
		
		LocalDate date = ((TimeValue<?, ?, ?, ?>) op).getEndDate().get();
		for (; i > 0; i--)
			date = date.plus(freq.getPeriod());
		for (; i < 0; i++)
			date = date.minus(freq.getPeriod());
		
		return DateValue.of(date);
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metashift = shift.getMetadata(scheme);
		VTLValueMetadata metaduration = duration.getMetadata(scheme);
		VTLValueMetadata meta = operand.getMetadata(scheme);
		
		if (!(metashift instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(metashift, ScalarValueMetadata.class);
		if (!INTEGERDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) metashift).getDomain()))
			throw new VTLIncompatibleTypesException("dateadd", INTEGERDS, ((ScalarValueMetadata<?, ?>) metashift).getDomain());
		if (!(metaduration instanceof ScalarValueMetadata))
			throw new VTLInvalidParameterException(metaduration, ScalarValueMetadata.class);
		if (!STRINGDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) metaduration).getDomain()))
			throw new VTLIncompatibleTypesException("dateadd", STRINGDS, ((ScalarValueMetadata<?, ?>) metashift).getDomain());
		
		if (meta instanceof ScalarValueMetadata)
		{
			ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) meta).getDomain();
			if (!(domain instanceof TimeDomainSubset))
				throw new VTLIncompatibleTypesException("dateadd", TIMEDS, domain);
		}
		else
			for (DataStructureComponent<Measure, ?, ?> comp: ((DataSetMetadata) meta).getMeasures())
				if (!(comp.getVariable().getDomain() instanceof TimeDomainSubset))
					throw new VTLIncompatibleTypesException("dateadd", TIMEDS, comp);
		
		return meta;
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		Set<LeafTransformation> set = new HashSet<>(operand.getTerminals());
		set.addAll(shift.getTerminals());
		set.addAll(duration.getTerminals());
		return set;
	}
	
	@Override
	public String toString()
	{
		return String.format("dateadd(%s, %s, %s)", operand, shift, duration); 
	}
}
