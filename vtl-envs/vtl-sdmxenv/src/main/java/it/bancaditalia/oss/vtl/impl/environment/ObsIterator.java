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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.TIME_PERIOD;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.sdmx.api.collection.KeyValue;
import io.sdmx.api.sdmx.engine.DataReaderEngine;
import io.sdmx.api.sdmx.model.data.Observation;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;

class ObsIterator implements Iterator<DataPoint>
{
	private final DataSetStructure structure;
	private final VTLAlias alias;
	private final DataReaderEngine dre;

	private boolean more;
	private Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dmap = new HashMap<>();
	private Entry<DateTimeFormatter, TemporalQuery<? extends TemporalAccessor>> parser;

	public ObsIterator(VTLAlias alias, DataReaderEngine dre, DataSetStructure structure)
	{
		this.structure = structure;
		this.alias = alias;
		this.dre = dre;
		
		if (structure.getMeasures().size() > 1)
			throw new UnsupportedOperationException("Unsupported dataset with multiple measures.");

		more = dre.moveNextDataset() && dre.moveNextKeyable();
		while (!dre.getCurrentKey().isSeries())
			more = dre.moveNextKeyable();
		more &= dre.moveNextObservation();
		if (more)
			setDims(dre, structure);
	}

	private void setDims(DataReaderEngine dre, DataSetStructure structure)
	{
		dmap.clear();
		// set ids for current ts dropping dimensions subbed in the query key
		for (KeyValue kv: dre.getCurrentKey().getKey())
		{
			String dimId = kv.getConcept();
			VTLAlias idAlias = VTLAliasImpl.of(VTLAlias.needsQuotes(dimId), dimId);
			Optional<DataSetComponent<Identifier, ?, ?>> maybeId = structure.getComponent(idAlias, Identifier.class);
			if (maybeId.isPresent())
			{
				DataSetComponent<Identifier, ?, ?> id = maybeId.get();
				dmap.put(id, id.getDomain().cast(StringValue.of(kv.getCode())));
			}
		}
		for (KeyValue k: dre.getCurrentKey().getAttributes())
		{
			DataSetComponent<Attribute, ?, ?> attr = structure.getComponent(VTLAliasImpl.of(true, k.getConcept()), Attribute.class)
					.orElseThrow(() -> new NoSuchElementException(k.getConcept()));
				dmap.put(attr, attr.getDomain().cast(StringValue.of(k.getCode())));
		}
	}

	@Override
	public boolean hasNext()
	{
		return more;
	}

	@Override
	public synchronized DataPoint next()
	{
		if (!more)
			throw new NoSuchElementException();
		
		Observation obs = dre.getCurrentObservation();
		DataPointBuilder builder = new DataPointBuilder(dmap, DONT_SYNC);

		if (parser == null)
			parser = SDMXEnvironment.getDateParser(obs.getDimensionValue());
		
		for (KeyValue a: obs.getAttributes())
		{
			DataSetComponent<?, ?, ?> c = structure.getComponent(VTLAliasImpl.of(true, a.getConcept())).get();
			builder.add(c, c.getDomain().cast(StringValue.of(a.getCode())));
		}
		
		DataSetComponent<Measure, ?, ?> measure = structure.getMeasures().iterator().next();
		List<String> values = obs.getMeasureValues(measure.getAlias().getName());
		if (values.size() > 1)
			throw new UnsupportedOperationException("Unsupported measure with multiple values (found " + values.size() + " values).");
		builder.add(measure, NumberValueImpl.createNumberValue(values.iterator().next()));

		TemporalAccessor parsed; 
		for (;;)
		{
			if (parser == null)
				parser = SDMXEnvironment.getDateParser(obs.getDimensionValue());
			
			try
			{
				parsed = parser.getKey().parse(obs.getDimensionValue());
				break;
			}
			catch (DateTimeParseException e)
			{
				parser = null;
			}
		}
		
		TemporalAccessor holder = parser.getValue().queryFrom(parsed);
		ScalarValue<?, ?, ?, ?> value;
		if (holder instanceof PeriodHolder)
			value = TimePeriodValue.of((PeriodHolder<?>) holder);
		else
			value = DateValue.of((LocalDate) holder);
		builder.add(TIME_PERIOD, value);
		
		if (!(more = dre.moveNextObservation()))
		{
			more = dre.moveNextKeyable();
			while (more && !dre.getCurrentKey().isSeries())
				more = dre.moveNextKeyable();
			more &= dre.moveNextObservation();
			
			if (!more)
			{
				more = dre.moveNextDataset() && dre.moveNextKeyable();
				while (more && !dre.getCurrentKey().isSeries())
					more = dre.moveNextKeyable();
				more &= dre.moveNextObservation();
			}
		
			if (more)
				setDims(dre, structure);
		}

		return builder.build(LineageExternal.of(alias.toString()), structure);
	}
}