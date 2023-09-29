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

import static it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory.registerSupportedProperties;
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.PASSWORD;
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.REQUIRED;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.MONTH_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.QUARTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.SEMESTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.YEAR_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static java.lang.Double.parseDouble;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Spliterator.IMMUTABLE;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
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
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.AbstractMap.SimpleEntry;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.sdmx.api.collection.KeyValue;
import io.sdmx.api.exception.SdmxUnauthorisedException;
import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.api.sdmx.engine.DataReaderEngine;
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
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.meta.fmr.FMRRepository;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DateHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimeDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class SDMXEnvironment implements Environment, Serializable
{
	public static final VTLProperty SDMX_DATA_ENDPOINT = new VTLPropertyImpl("vtl.sdmx.data.endpoint", "SDMX 2.1 data REST base URL", "https://www.myurl.com/service", EnumSet.of(REQUIRED));
	public static final VTLProperty SDMX_DATA_USERNAME = new VTLPropertyImpl("vtl.sdmx.data.user", "SDMX Data Provider user name", "", EnumSet.noneOf(Flags.class));
	public static final VTLProperty SDMX_DATA_PASSWORD = new VTLPropertyImpl("vtl.sdmx.data.password", "SDMX Data Provider password", "", EnumSet.of(PASSWORD));

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SDMXEnvironment.class); 
	private static final Map<DateTimeFormatter, TemporalQuery<? extends TemporalAccessor>> FORMATTERS = new HashMap<>();
	private static final DataStructureComponent<Identifier,EntireTimeDomainSubset,TimeDomain> TIME_PERIOD = DataStructureComponentImpl.of("TIME_PERIOD", Identifier.class, TIMEDS);
	private static final DataReaderManager DR_MANAGER = new DataReaderManagerImpl(new DataFormatManagerImpl(null, new InformationFormatManager()));
	private static final SdmxSourceReadableDataLocationFactory RDL_FACTORY = new SdmxSourceReadableDataLocationFactory();

	private final FMRRepository repo;
	private final String endpoint = SDMX_DATA_ENDPOINT.getValue();
	private final boolean drop;

	static
	{
		registerSupportedProperties(SDMXEnvironment.class, SDMX_DATA_ENDPOINT, SDMX_DATA_USERNAME, SDMX_DATA_PASSWORD);
		
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"), DateHolder::of);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm"), DateHolder::of);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd hh"), DateHolder::of);
		FORMATTERS.put(DateTimeFormatter.ofPattern("yyyy-MM-dd"), DateHolder::of);
		FORMATTERS.put(YEAR_PERIOD_FORMATTER.get(), YearPeriodHolder::new);
		FORMATTERS.put(SEMESTER_PERIOD_FORMATTER.get(), SemesterPeriodHolder::new);
		FORMATTERS.put(QUARTER_PERIOD_FORMATTER.get(), QuarterPeriodHolder::new);
		FORMATTERS.put(MONTH_PERIOD_FORMATTER.get(), MonthPeriodHolder::new);
	}
	
	public SDMXEnvironment() throws URISyntaxException
	{
		LOGGER.info("Initializing SDMX Environment...");
		if (endpoint == null || endpoint.isEmpty())
			throw new IllegalStateException("No endpoint configured for SDMX Environment.");
		
		MetadataRepository maybeRepo = ConfigurationManagerFactory.getInstance().getMetadataRepository();
		if (maybeRepo instanceof FMRRepository)
			repo = (FMRRepository) maybeRepo;
		else
			throw new IllegalStateException("The SDMX Environment must be used with the FMR Metadata Repository.");

		LOGGER.info("Loading SDMX data from {}", endpoint);

		drop = repo.isDrop();
		
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
	public boolean contains(String alias)
	{
		return repo.getStructure(alias) != null;
	}

	@Override
	public Optional<VTLValue> getValue(String alias)
	{
		DataSetMetadata structure = repo.getStructure(alias);
		
		if (structure == null)
			return Optional.empty();
		
		String[] query = alias.split("/", 2);
		String dataflow = query[0].replace(':', ',').replace('(', ',').replace(')', '/');
		String resource = query[1];
		
		String[] dims = drop && alias.indexOf('/') > 0 ? resource.split("\\.") : new String[] {};

		String path = endpoint + "/data/" + dataflow + resource;
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
				urlc.addRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((SDMX_DATA_USERNAME.getValue() + ":" + SDMX_DATA_PASSWORD.getValue()).getBytes()));
				((HttpURLConnection) urlc).setInstanceFollowRedirects(true);
				InputStream is = urlc.getInputStream();
				rdl = RDL_FACTORY.getReadableDataLocation("gzip".equals(urlc.getContentEncoding()) ? new GZIPInputStream(is) : is);
			}
			catch (IOException | URISyntaxException e1)
			{
				throw new VTLException("Error in creating readableDataLocation", e);
			}
		}
		
		DataReaderEngine dre = DR_MANAGER.getDataReaderEngine(rdl, repo.getBeanRetrievalManager(), new FirstFailureErrorHandler());
		
		return Optional.of(new AbstractDataSet(structure) {
			private static final long serialVersionUID = 1L;

			@Override
			protected Stream<DataPoint> streamDataPoints()
			{
				return StreamSupport.stream(Spliterators.spliterator(new ObsIterator(alias, dre, structure, dims), 20, IMMUTABLE), false);
			}
		});
	}

	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String alias)
	{
		Optional<VTLValueMetadata> structure = Optional.ofNullable(repo.getStructure(alias));
		if (!structure.isPresent())
			LOGGER.info("No structure in FMR corresponding to {}", alias);
		return structure;
	}

	private static class ObsIterator implements Iterator<DataPoint>
	{
		private final String[] dims;
		private final DataSetMetadata structure;
		private final String alias;
		private final DataReaderEngine dre;

		private boolean no;
		private Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> dmap = new HashMap<>();
		private SimpleEntry<DateTimeFormatter, TemporalQuery<? extends TemporalAccessor>> parser;

		public ObsIterator(String alias, DataReaderEngine dre, DataSetMetadata structure, String[] dims)
		{
			this.dims = dims;
			this.structure = structure;
			this.alias = alias;
			this.dre = dre;

			no = dre.moveNextDataset() && dre.moveNextKeyable() && dre.moveNextObservation();
			if (no)
				setDims(dre, structure);
		}

		private void setDims(DataReaderEngine dre, DataSetMetadata structure)
		{
			dmap.clear();
			List<KeyValue> keys = dre.getCurrentKey().getKey();
			for (int i = 0; i < keys.size(); i++)
				if (i >= dims.length || dims[i].isEmpty() || dims[i].indexOf('+') >= 0)
				{
					KeyValue k = keys.get(i);
					DataStructureComponent<Identifier, ?, ?> dim = structure.getComponent(k.getConcept(), Identifier.class)
							.orElseThrow(() -> new NoSuchElementException(k.getConcept()));
						dmap.put(dim, dim.cast(StringValue.of(k.getCode())));
				}
			for (KeyValue k: dre.getCurrentKey().getAttributes())
			{
				DataStructureComponent<Attribute, ?, ?> attr = structure.getComponent(k.getConcept(), Attribute.class)
						.orElseThrow(() -> new NoSuchElementException(k.getConcept()));
					dmap.put(attr, attr.cast(StringValue.of(k.getCode())));
			}
		}

		@Override
		public boolean hasNext()
		{
			return no;
		}

		@Override
		public synchronized DataPoint next()
		{
			Observation obs = dre.getCurrentObservation();
			DataPointBuilder builder = new DataPointBuilder(dmap);

			if (parser == null)
				parser = getDateParser(obs.getDimensionValue());
			
			for (KeyValue a: obs.getAttributes())
			{
				DataStructureComponent<?, ?, ?> c = structure.getComponent(a.getConcept()).get();
				builder.add(c, c.cast(StringValue.of(a.getCode())));
			}
			
			DataStructureComponent<Measure, ?, ?> measure = structure.getComponents(Measure.class).iterator().next();
			builder.add(measure, DoubleValue.of(parseDouble(obs.getMeasureValue(measure.getName()))));
			TemporalAccessor holder = parser.getValue().queryFrom(parser.getKey().parse(obs.getDimensionValue()));
			ScalarValue<?, ?, ?, ?> value;
			if (holder instanceof PeriodHolder)
				value = TimePeriodValue.of((PeriodHolder<?>) holder);
			else
				value = DateValue.of((DateHolder<?>) holder);
			builder.add(TIME_PERIOD, value);
			
			if (!(no = dre.moveNextObservation()))
			{
				if (!(no = dre.moveNextKeyable() && dre.moveNextObservation()))
					no = dre.moveNextDataset() && dre.moveNextKeyable() && dre.moveNextObservation();
			
				if (no)
					setDims(dre, structure);
			}

			return builder.build(LineageExternal.of(alias), structure);
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
}
