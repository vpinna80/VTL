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

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getLocalPropertyValue;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.registerSupportedProperties;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_PASSWORD;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_URL;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.MONTH_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.QUARTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.SEMESTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.YEAR_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.TIME_PERIOD;
import static it.bancaditalia.oss.vtl.util.Utils.SEQUENTIAL;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterators.spliteratorUnknownSize;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.AbstractMap.SimpleEntry;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.sdmx.api.collection.KeyValue;
import io.sdmx.api.exception.SdmxUnauthorisedException;
import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.api.sdmx.engine.DataReaderEngine;
import io.sdmx.api.sdmx.manager.structure.SdmxBeanRetrievalManager;
import io.sdmx.api.sdmx.model.data.Observation;
import io.sdmx.core.data.api.manager.DataReaderManager;
import io.sdmx.core.data.manager.DataFormatManagerImpl;
import io.sdmx.core.data.manager.DataReaderManagerImpl;
import io.sdmx.core.sdmx.error.FirstFailureErrorHandler;
import io.sdmx.core.sdmx.manager.format.InformationFormatManager;
import io.sdmx.core.sdmx.manager.structure.StructureReaderManagerImpl;
import io.sdmx.format.ml.factory.data.SdmxMLDataFormatFactory;
import io.sdmx.format.ml.factory.data.SdmxMLDataReaderFactory;
import io.sdmx.format.ml.factory.structure.SdmxMLStructureReaderFactory;
import io.sdmx.fusion.service.builder.StructureQueryBuilderRest;
import io.sdmx.fusion.service.engine.RESTQueryBrokerEngineImpl;
import io.sdmx.utils.core.application.SingletonStore;
import io.sdmx.utils.core.io.SdmxSourceReadableDataLocationFactory;
import io.sdmx.utils.http.api.model.IHttpProxy;
import io.sdmx.utils.http.broker.RestMessageBroker;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class SDMXEnvironment implements Environment, Serializable
{
	public static final VTLProperty SDMX_DATA_ENDPOINT = new VTLPropertyImpl("vtl.sdmx.data.endpoint", "SDMX REST data base URL", "https://www.myurl.com/service", EnumSet.of(IS_REQUIRED, IS_URL));
	public static final VTLProperty SDMX_DATA_USERNAME = new VTLPropertyImpl("vtl.sdmx.data.user", "SDMX Data Provider user name", "", emptySet());
	public static final VTLProperty SDMX_DATA_PASSWORD = new VTLPropertyImpl("vtl.sdmx.data.password", "SDMX Data Provider password", "", EnumSet.of(IS_PASSWORD));

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SDMXEnvironment.class); 
	private static final Map<DateTimeFormatter, TemporalQuery<? extends TemporalAccessor>> FORMATTERS = new HashMap<>();
	private static final SdmxSourceReadableDataLocationFactory RDL_FACTORY = new SdmxSourceReadableDataLocationFactory();
	private static final Pattern SDMX_DATAFLOW_PATTERN = Pattern.compile("^(?:(?<agency>[A-Za-z_][A-Za-z0-9_.]*):)(?<dataflow>[A-Za-z_][A-Za-z0-9_.]*)(?:\\((?<version>[0-9._+*~]+)\\))?(?::(?<query>(?:\\.|[A-Za-z_][A-Za-z0-9_]*)+))?$");

	static
	{
		registerSupportedProperties(SDMXEnvironment.class, SDMX_DATA_ENDPOINT, SDMX_DATA_USERNAME, SDMX_DATA_PASSWORD);
		
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"), LocalDate::from);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm"), LocalDate::from);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh"), LocalDate::from);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd"), LocalDate::from);
		FORMATTERS.put(MONTH_PERIOD_FORMATTER.get(), MonthPeriodHolder::new);
		FORMATTERS.put(QUARTER_PERIOD_FORMATTER.get(), QuarterPeriodHolder::new);
		FORMATTERS.put(SEMESTER_PERIOD_FORMATTER.get(), SemesterPeriodHolder::new);
		FORMATTERS.put(YEAR_PERIOD_FORMATTER.get(), YearPeriodHolder::new);
	}

	private final String endpoint;
	private final String username;
	private final String password;

	public SDMXEnvironment() throws URISyntaxException
	{
		this(getLocalPropertyValue(SDMX_DATA_ENDPOINT), getLocalPropertyValue(SDMX_DATA_USERNAME), getLocalPropertyValue(SDMX_DATA_PASSWORD));
	}

	public SDMXEnvironment(String endpoint, String username, String password) throws URISyntaxException
	{
		LOGGER.info("Initializing SDMX Environment...");
		if (endpoint == null || endpoint.isEmpty())
			throw new IllegalStateException("No endpoint configured for SDMX Environment.");
		
		LOGGER.info("SDMX data will be loaded from {}", endpoint);

		this.endpoint = endpoint;
		this.username = username;
		this.password = password;

		// FMR client configuration
		SingletonStore.registerInstance(new RESTQueryBrokerEngineImpl());
		SingletonStore.registerInstance(new StructureQueryBuilderRest());
		SingletonStore.registerInstance(new StructureReaderManagerImpl());
		SdmxMLStructureReaderFactory.registerInstance();
		SdmxMLDataFormatFactory.registerInstance();
		SdmxMLDataReaderFactory.registerInstance();

		URI uri = new URI(endpoint);
		Proxy proxy = ProxySelector.getDefault().select(uri).get(0);
		if (proxy.type() == Type.HTTP)
		{
			String proxyHost = ((InetSocketAddress) proxy.address()).getHostName();
			LOGGER.info("Fetching SDMX data through proxy {}", proxyHost);
			RestMessageBroker.setProxies(singletonMap(uri.getHost(), 
			new IHttpProxy() {
				@Override public String getProxyUser() { return null; }
				@Override public String getProxyUrl() { return proxyHost; }
				@Override public Integer getProxyPort() { return ((InetSocketAddress) proxy.address()).getPort(); }
				@Override public String getProxyPassword() { return null; }
				@Override public String getDomain() { return null; }
				@Override public String getDecryptedPassword() { return null; }
			}));
		}
		else
			RestMessageBroker.setProxies(emptyMap());
		
		LOGGER.info("SDMX Environment initialization complete.");
	}
	
	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias alias)
	{
		Matcher matcher = SDMX_DATAFLOW_PATTERN.matcher(alias.getName());
		if (!matcher.matches())
			return Optional.empty();
		
		Optional<DataSetStructure> maybeMeta = repo.getMetadata(alias).map(DataSetStructure.class::cast);
		if (maybeMeta.isEmpty())
			return Optional.empty();
		
		DataSetStructure structure = maybeMeta.get();
		
		String version = coalesce(matcher.group("version"), "");
		String dataflow;
		if (version.isEmpty())
			dataflow = matcher.group("agency") + "," + matcher.group("dataflow");
		else
			dataflow = matcher.group("agency") + "," + matcher.group("dataflow") + "," + version;

		String resource = coalesce(matcher.group("query"), "");
		String[] dims = resource.isEmpty() ? new String[] {} : resource.split("\\.");

		AbstractDataSet sdmxDataflow = new StreamWrapperDataSet(structure, () -> getData(repo, alias, structure, dataflow, resource, dims));
		return Optional.of(sdmxDataflow);
	}

	@Override
	public Optional<VTLValueMetadata> getValueMetadata(VTLAlias alias)
	{
		throw new IllegalStateException("SDMXEnvironment.getValueMetadata should never be called.");
	}

	private static class ObsIterator implements Iterator<DataPoint>
	{
		private final String[] dims;
		private final DataSetStructure structure;
		private final VTLAlias alias;
		private final DataReaderEngine dre;

		private boolean more;
		private Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dmap = new HashMap<>();
		private Entry<DateTimeFormatter, TemporalQuery<? extends TemporalAccessor>> parser;

		public ObsIterator(VTLAlias alias, DataReaderEngine dre, DataSetStructure structure, String[] dims)
		{
			this.dims = dims;
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
			List<KeyValue> keys = dre.getCurrentKey().getKey();
			for (int i = 0; i < keys.size(); i++)
				if (i >= dims.length || dims[i].isEmpty() || dims[i].indexOf('+') >= 0)
				{
					KeyValue k = keys.get(i);
					DataSetComponent<Identifier, ?, ?> dim = structure.getComponent(VTLAliasImpl.of(true, k.getConcept()), Identifier.class)
							.orElseThrow(() -> new NoSuchElementException(k.getConcept()));
						dmap.put(dim, dim.getDomain().cast(StringValue.of(k.getCode())));
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
			Observation obs = dre.getCurrentObservation();
			DataPointBuilder builder = new DataPointBuilder(dmap, DONT_SYNC);

			if (parser == null)
				parser = getDateParser(obs.getDimensionValue());
			
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
					parser = getDateParser(obs.getDimensionValue());
				
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

	private static SimpleEntry<DateTimeFormatter, TemporalQuery<? extends TemporalAccessor>> getDateParser(String dateStr)
	{
		DateTimeException last = null;
		for (DateTimeFormatter formatter : FORMATTERS.keySet())
			try
			{
				formatter.parse(dateStr, FORMATTERS.get(formatter));
				return new SimpleEntry<>(formatter, FORMATTERS.get(formatter));
			}
			catch (DateTimeException e)
			{
				last = e;
			}

		throw last;
	}

	private synchronized Stream<DataPoint> getData(MetadataRepository repo, VTLAlias alias, DataSetStructure structure, String dataflow, String resource, String[] dims)
	{
		String path = endpoint + "/data/" + dataflow;
		if (!resource.isEmpty())
			 path += "/" + resource;
		
		ReadableDataLocation rdl;
		try 
		{
			rdl = RDL_FACTORY.getReadableDataLocation(path);
		}
		catch (SdmxUnauthorisedException e)
		{
			try
			{
				URL url = new URI(path).toURL();
				URLConnection urlc = url.openConnection();
				urlc.setDoOutput(true);
				urlc.setAllowUserInteraction(false);
				urlc.addRequestProperty("Accept-Encoding", "gzip");
				urlc.addRequestProperty("Accept", "*/*;q=1.0");
				urlc.addRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes()));
				((HttpURLConnection) urlc).setInstanceFollowRedirects(true);
				InputStream is = urlc.getInputStream();
				rdl = RDL_FACTORY.getReadableDataLocation("gzip".equals(urlc.getContentEncoding()) ? new GZIPInputStream(is) : is);
			}
			catch (IOException | URISyntaxException e1)
			{
				throw new VTLException("Error in creating readableDataLocation", e);
			}
		}

		SdmxBeanRetrievalManager brm = null;
		try
		{
			// This is a field present only in SDMXMetadataRepository and SDMXJsonMetadataRepository classes 
			Field field = repo.getClass().getField("rbrm");
			brm = (SdmxBeanRetrievalManager) field.get(repo);
		}
		catch (SecurityException | IllegalAccessException | IllegalArgumentException | NoSuchFieldException e)
		{
			throw new IllegalStateException("The SDMX Environment must be used with a SDMX Metadata Repository.");
		}
		
		DataReaderManager manager = new DataReaderManagerImpl(new DataFormatManagerImpl(null, new InformationFormatManager()));
		DataReaderEngine dre = manager.getDataReaderEngine(rdl, brm, new FirstFailureErrorHandler());
		return StreamSupport.stream(spliteratorUnknownSize(new ObsIterator(alias, dre, structure, dims), IMMUTABLE), SEQUENTIAL);
	}
}
