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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.sdmx.api.PortableDataSet.OBS_LABEL;
import static it.bancaditalia.oss.sdmx.api.PortableDataSet.TIME_LABEL;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.MONTH_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.QUARTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.SEMESTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.YEAR_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.util.Utils.byKey;
import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;
import static java.util.stream.Collectors.toConcurrentMap;

import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import it.bancaditalia.oss.sdmx.api.BaseObservation;
import it.bancaditalia.oss.sdmx.api.Codelist;
import it.bancaditalia.oss.sdmx.api.DataFlowStructure;
import it.bancaditalia.oss.sdmx.api.Dimension;
import it.bancaditalia.oss.sdmx.api.PortableDataSet;
import it.bancaditalia.oss.sdmx.api.PortableTimeSeries;
import it.bancaditalia.oss.sdmx.api.SdmxMetaElement;
import it.bancaditalia.oss.sdmx.client.SdmxClientHandler;
import it.bancaditalia.oss.sdmx.exceptions.DataStructureException;
import it.bancaditalia.oss.sdmx.exceptions.SdmxException;
import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointImpl.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureImpl.Builder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringCodeList;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class SDMXEnvironment implements Environment
{
	public static final String DROP_ID_PROPERTY = "vtl.sdmx.keep.identifiers";
	
	private static final Set<String> UNSUPPORTED = Stream.of("CONNECTORS_AUTONAME", "action", "validFromDate", "ID").collect(Collectors.toSet());
	private static final SortedMap<String, Boolean> PROVIDERS = SdmxClientHandler.getProviders(); // it will contain only built-in providers for now.
	private static final Map<DateTimeFormatter, Function<TemporalAccessor, TimeValue<?, ?, ?>>> FORMATTERS = new HashMap<>();

	private final boolean dropIdentifiers = !"true".equalsIgnoreCase(System.getProperty(DROP_ID_PROPERTY, "false"));
	private final MetadataRepository repository = ConfigurationManager.getDefaultFactory().getMetadataRepositoryInstance();

	static
	{
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"), DateValue::new);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm"), DateValue::new);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh"), DateValue::new);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd"), DateValue::new);
		FORMATTERS.put(YEAR_PERIOD_FORMATTER.get(), TimePeriodValue::new);
		FORMATTERS.put(SEMESTER_PERIOD_FORMATTER.get(), TimePeriodValue::new);
		FORMATTERS.put(QUARTER_PERIOD_FORMATTER.get(), TimePeriodValue::new);
		FORMATTERS.put(MONTH_PERIOD_FORMATTER.get(), TimePeriodValue::new);
	}

	@Override
	public boolean contains(String id)
	{
		return id.startsWith("sdmx:") && PROVIDERS.containsKey(id.substring(5).split("\\.")[0]);
	}

	@Override
	public Optional<VTLValue> getValue(String name)
	{
		if (contains(name))
		{
			name = name.substring(5);
			String provider = name.split("\\.")[0];
			String query = name.split("\\.", 2)[1];
			try
			{
				List<PortableTimeSeries<Double>> table = SdmxClientHandler.getTimeSeries(provider, query, null, null);
				return Optional.of(parseSDMXTable(name, table));
			}
			catch (SdmxException | DataStructureException e)
			{
				throw new VTLNestedException("Fatal error contacting SDMX provider '" + provider + "'", e);
			}
		}

		return Optional.empty();
	}

	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String name)
	{
		if (contains(name))
		{
			name = name.substring(5);
			String provider = name.split("\\.")[0];
			String dataflow = name.split("\\.")[1];

			return getMetadataSDMX(provider, dataflow, name.split("\\.", 3)[2].split("\\.", -1));
		}

		return Optional.empty();
	}

	protected DataSet parseSDMXTable(String name, List<PortableTimeSeries<Double>> table) throws DataStructureException
	{
		VTLDataSetMetadata metadata = (VTLDataSetMetadata) getValueMetadata("sdmx:" + name)
				.orElseThrow(() -> new NullPointerException("Could not retrieve SDMX metadata for " + name));

		ConcurrentMap<PortableTimeSeries<Double>, ? extends ConcurrentMap<String, ? extends ScalarValue<?, ?, ?>>> seriesMeta = Utils.getStream(table)
				.map(toEntry(Function.identity(), PortableTimeSeries::getAttributesMap))
				.map(e -> {
					ConcurrentMap<String, String> attrs = new ConcurrentHashMap<>(e.getKey().getAttributesMap());
					attrs.putAll(e.getKey().getDimensionsMap());
					return new SimpleEntry<>(e.getKey(), attrs);
				})
				.map(keepingKey(SDMXEnvironment::extractAttrs))
				.collect(entriesToMap());

		return new LightDataSet(metadata, () -> Utils.getStream(table)
				.map(s -> s.stream()
						.map(o -> l1(metadata, seriesMeta, s, o)) //
						.map(v -> new DataPointBuilder(v).build(metadata))
				).reduce(Stream::concat)
				.orElse(Stream.empty()));
	}

	private ConcurrentMap<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?>> l1(VTLDataSetMetadata metadata,
			ConcurrentMap<? extends PortableTimeSeries<Double>, ? extends ConcurrentMap<String, ? extends ScalarValue<?, ?, ?>>> seriesMeta,
			PortableTimeSeries<Double> s, BaseObservation<? extends Double> o)
	{
		return Stream.concat(seriesMeta.get(s).entrySet().stream(),
				Stream.concat(obsLevelAttrs(s, o),
						Stream.of(new SimpleEntry<>(TIME_LABEL, asDate(o)),
								new SimpleEntry<>(OBS_LABEL, new DoubleValue(o.getValueAsDouble())))))
				.map(e -> new SimpleEntry<>(metadata.getComponent(e.getKey()).get(), e.getValue()))
				.map(keepingKey((k, v) -> k.getDomain().cast(v)))
				.collect(toConcurrentMap(Entry::getKey, Entry::getValue));
	}

	private ScalarValue<?, ? extends TimeDomainSubset<? extends TimeDomain>, ? extends TimeDomain> asDate(BaseObservation<? extends Double> o)
	{
		DateTimeException last = null;
		for (DateTimeFormatter formatter : FORMATTERS.keySet())
			try
			{
				TemporalAccessor parsed = formatter.parse(o.getTimeslot());
				return FORMATTERS.get(formatter).apply(parsed);
			}
			catch (DateTimeException e)
			{
				last = e;
			}

		if (last != null)
			throw last;
		else 
			throw new IllegalStateException("this point should not be reached");
	}

	private static ConcurrentMap<String, ? extends ScalarValue<?, ?, ?>> extractAttrs(Map<String, String> attrs)
	{
		return Utils.getStream(attrs.entrySet())
				.filter(e -> !UNSUPPORTED.contains(e.getKey()))
				.map(keepingKey(StringValue::new))
				.map(keepingKey(v -> (ScalarValue<?, ?, ?>) v))
				.collect(entriesToMap());
	}

	private static Stream<Entry<String, ScalarValue<?, ?, ?>>> obsLevelAttrs(PortableTimeSeries<Double> series, BaseObservation<? extends Double> observation)
	{
		return Utils.getStream(observation.getAttributes().entrySet())
				.filter(byKey(k -> !UNSUPPORTED.contains(k)))
				.map(keepingKey(v -> (ScalarValue<?, ?, ?>) (v != null ? new StringValue(v) : NullValue.instance(STRINGDS))));
	}

	private <T extends ComponentRole> DataStructureComponentImpl<T, ValueDomainSubset<StringDomain>, StringDomain> elementToComponent(Class<T> role,
			SdmxMetaElement meta)
	{
		Codelist codelist = meta.getCodeList();
		if (codelist == null)
			return new DataStructureComponentImpl<>(meta.getId(), role, STRINGDS);

		String domainName = codelist.getId();
		StringCodeList vtlCodelist = repository.defineDomain(domainName, StringCodeList.class, codelist.keySet());
		return new DataStructureComponentImpl<>(meta.getId(), role, vtlCodelist);
	}

	protected Optional<VTLValueMetadata> getMetadataSDMX(String provider, String dataflow, String[] tokens)
	{
		try
		{
			DataFlowStructure dsd = SdmxClientHandler.getDataFlowStructure(provider, dataflow);

			// remove the fixed (not wildcarded) dimensions from the list of identifiers
			List<Dimension> dimensions = dsd.getDimensions();
			for (Dimension d : dimensions)
				SdmxClientHandler.getCodes(provider, dataflow, d.getId());

			List<SdmxMetaElement> activeAttributes = new ArrayList<>(dsd.getAttributes());
			List<Dimension> activeDims = new ArrayList<>();

			if (isDropIdentifiers())
				for (int i = 0; i < tokens.length; i++)
					if (tokens[i].isEmpty() || tokens[i].indexOf('+') != -1)
						activeDims.add(dimensions.get(i));
					else
						activeAttributes.add(dimensions.get(i));
			else
				activeDims = dimensions;

			return Optional.of(Stream
					.concat(activeDims.stream().map(d -> elementToComponent(Identifier.class, d)),
							activeAttributes.stream().map(a -> elementToComponent(Attribute.class, a)))
					.reduce(new Builder(), Builder::addComponent, Builder::merge)
					.addComponent(new DataStructureComponentImpl<>(PortableDataSet.TIME_LABEL, Identifier.class, TIMEDS))
					.addComponent(new DataStructureComponentImpl<>(PortableDataSet.OBS_LABEL, Measure.class, NUMBERDS)).build());
		}
		catch (SdmxException e)
		{
			throw new VTLException("SDMX", e);
		}
	}

	public boolean isDropIdentifiers()
	{
		return dropIdentifiers;
	}
}
