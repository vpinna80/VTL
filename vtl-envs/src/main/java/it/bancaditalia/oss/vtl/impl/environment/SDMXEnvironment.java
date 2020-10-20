/**
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

import static it.bancaditalia.oss.sdmx.api.PortableDataSet.OBS_LABEL;
import static it.bancaditalia.oss.sdmx.api.PortableDataSet.TIME_LABEL;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.MONTH_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.QUARTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.SEMESTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.YEAR_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringEnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class SDMXEnvironment implements Environment, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SDMXEnvironment.class); 
	private static final DataStructureComponentImpl<Measure, NumberDomainSubset<NumberDomain>, NumberDomain> OBS_VALUE_MEASURE = new DataStructureComponentImpl<>(PortableDataSet.OBS_LABEL, Measure.class, NUMBERDS);
	private static final DataStructureComponentImpl<Identifier, TimeDomainSubset<TimeDomain>, TimeDomain> TIME_PERIOD_IDENTIFIER = new DataStructureComponentImpl<>(PortableDataSet.TIME_LABEL, Identifier.class, TIMEDS);
	private static final Set<String> UNSUPPORTED = Stream.of("CONNECTORS_AUTONAME", "action", "validFromDate", "ID").collect(toSet());
	private static final SortedMap<String, Boolean> PROVIDERS = SdmxClientHandler.getProviders(); // it will contain only built-in providers for now.
	private static final Map<DateTimeFormatter, Function<TemporalAccessor, TimeValue<?, ?, ?>>> FORMATTERS = new HashMap<>();
	private static final Pattern SDMX_PATTERN = Pattern.compile("^(.+):(?:(.+?)(?:\\((.+)\\))?)/(.+)$");

	public static final VTLProperty SDMX_ENVIRONMENT_AUTODROP_IDENTIFIERS = 
			new VTLPropertyImpl("vtl.sdmx.keep.identifiers", "True to keep subspaced identifiers", "false", false, false, "false");

	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(SDMXEnvironment.class, SDMX_ENVIRONMENT_AUTODROP_IDENTIFIERS);
		
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
	public boolean contains(String name)
	{
		return getMatcher(name).isPresent();
	}

	private Optional<Matcher> getMatcher(String name)
	{
		return Optional.of(SDMX_PATTERN.matcher(name))
				.filter(matcher -> matcher.matches() && PROVIDERS.containsKey(matcher.group(1)));
	}

	@Override
	public Optional<VTLValue> getValue(String name)
	{
		return getMatcher(name)
			.map(matcher ->	{
				String provider = matcher.group(1);
				String dataflow = matcher.group(2);
				String query = dataflow + "/" + matcher.group(4);
				try
				{
					List<PortableTimeSeries<Double>> table = SdmxClientHandler.getTimeSeries(provider, query, null, null);
					return parseSDMXTable(name, table);
				}
				catch (SdmxException | DataStructureException e)
				{
					throw new VTLNestedException("Fatal error contacting SDMX provider '" + provider + "'", e);
				}
			});
	}

	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String name)
	{
		return getMatcher(name)
			.map(matcher ->	{
				String provider = matcher.group(1);
				String dataflow = matcher.group(2);
				String query = matcher.group(4);

				return getMetadataSDMX(provider, dataflow, query.split("\\."));
			});
	}

	protected DataSet parseSDMXTable(String name, List<PortableTimeSeries<Double>> table) throws DataStructureException
	{
		DataSetMetadata metadata = (DataSetMetadata) getValueMetadata(name)
				.orElseThrow(() -> new NullPointerException("Could not retrieve SDMX metadata for " + name));

		Map<PortableTimeSeries<Double>, Map<String, ScalarValue<?, ?, ?>>> seriesMeta = Utils.getStream(table)
				.map(toEntry(Function.identity(), PortableTimeSeries::getAttributesMap))
				.map(e -> {
					ConcurrentMap<String, String> attrs = new ConcurrentHashMap<>(e.getKey().getAttributesMap());
					attrs.putAll(e.getKey().getDimensionsMap());
					return new SimpleEntry<>(e.getKey(), attrs);
				})
				.map(keepingKey(SDMXEnvironment::extractAttrs))
				.collect(entriesToMap());

		return new LightFDataSet<>(metadata, t -> Utils.getStream(t) // for each series
				.map(s -> s.stream() // build a dp
						.map(o -> obsToCompValues(seriesMeta.get(s), o)
							.map(keepingValue(k -> (DataStructureComponent<?, ?, ?>) metadata.getComponent(k).get()))
							.map(keepingKey((k, v) -> k.getDomain().cast(v)))
							.collect(DataPointBuilder.toDataPoint(metadata))))
				.reduce(Stream::concat)
				.orElse(Stream.empty()), table);
	}

	private static Stream<Entry<String, ScalarValue<?, ?, ?>>> obsToCompValues(Map<String, ScalarValue<?,?,?>> seriesLevelAttrs, 
			BaseObservation<? extends Double> o)
	{
		return Stream.concat(Utils.getStream(seriesLevelAttrs), Stream.concat(obsLevelAttrs(o),
						Stream.of(new SimpleEntry<>(TIME_LABEL, asDate(o)),
								new SimpleEntry<>(OBS_LABEL, new DoubleValue(o.getValueAsDouble())))));
	}

	private static ScalarValue<?, ? extends TimeDomainSubset<? extends TimeDomain>, ? extends TimeDomain> asDate(BaseObservation<? extends Double> o)
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

	private static Map<String, ScalarValue<?, ?, ?>> extractAttrs(Map<String, String> attrs)
	{
		return Utils.getStream(attrs.entrySet())
				.filter(e -> !UNSUPPORTED.contains(e.getKey()))
				.map(keepingKey(StringValue::new))
				.map(keepingKey(v -> (ScalarValue<?, ?, ?>) v))
				.collect(entriesToMap());
	}

	private static Stream<Entry<String, ScalarValue<?, ?, ?>>> obsLevelAttrs(BaseObservation<? extends Double> observation)
	{
		return Utils.getStream(observation.getAttributes())
				.filter(entryByKey(k -> !UNSUPPORTED.contains(k)))
				.map(keepingKey(v -> (ScalarValue<?, ?, ?>) (v != null ? new StringValue(v) : NullValue.instance(STRINGDS))));
	}

	private static <T extends Component> DataStructureComponent<T, StringDomainSubset, StringDomain> elementToComponent(Class<T> role,
			SdmxMetaElement meta)
	{
		Codelist codelist = meta.getCodeList();
		if (codelist == null)
			return new DataStructureComponentImpl<>(meta.getId(), role, STRINGDS);
		
		MetadataRepository repository = ConfigurationManager.getDefault().getMetadataRepository();
		StringEnumeratedDomainSubset domain = repository.defineDomain(codelist.getId(), StringEnumeratedDomainSubset.class, codelist.keySet());
		Objects.requireNonNull(domain, "domain null for " + codelist.getId() + " - " + meta);
		return new DataStructureComponentImpl<>(meta.getId(), role, domain);
	}

	protected VTLValueMetadata getMetadataSDMX(String provider, String dataflow, String[] tokens)
	{
		try
		{
			LOGGER.trace("Retrieving DSD for {}:{}", provider, dataflow);
			DataFlowStructure dsd = SdmxClientHandler.getDataFlowStructure(provider, dataflow);

			// Load all the codes of each dimension
			List<Dimension> dimensions = dsd.getDimensions();
			if (tokens.length != dimensions.size())
				throw new InvalidParameterException("Query items " + Arrays.toString(tokens) + " do not match the dimensions of " 
						+ dataflow + " " + dimensions.stream().map(Dimension::getId).collect(toList()));
			for (Dimension d: dimensions)
			{
				String dimId = d.getId();
				LOGGER.trace("Retrieving codelist for dimension {} of {}:{}", dimId, provider, dataflow);
				SdmxClientHandler.getCodes(provider, dataflow, dimId);
			}
			

			// remove the fixed (not wildcarded) dimensions from the list of identifiers
			List<SdmxMetaElement> activeAttributes = new ArrayList<>(dsd.getAttributes());
			List<Dimension> activeDims = new ArrayList<>();

			if (!"true".equals(SDMX_ENVIRONMENT_AUTODROP_IDENTIFIERS.getValue()))
				for (int i = 0; i < tokens.length; i++)
					if (tokens[i].isEmpty() || tokens[i].indexOf('+') != -1)
						activeDims.add(dimensions.get(i));
					else
						activeAttributes.add(dimensions.get(i));
			else
				activeDims = dimensions;

			return Stream
					.concat(activeDims.stream().map(d -> elementToComponent(Identifier.class, d)),
							activeAttributes.stream().map(a -> elementToComponent(Attribute.class, a)))
					.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge)
					.addComponent(TIME_PERIOD_IDENTIFIER)
					.addComponent(OBS_VALUE_MEASURE)
					.build();
		}
		catch (SdmxException e)
		{
			throw new VTLException("SDMX", e);
		}
	}
}
