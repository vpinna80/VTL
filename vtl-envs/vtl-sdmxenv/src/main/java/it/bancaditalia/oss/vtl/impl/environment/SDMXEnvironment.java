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
import static it.bancaditalia.oss.vtl.util.Utils.SEQUENTIAL;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterators.spliteratorUnknownSize;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.AbstractMap.SimpleEntry;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.sdmx.api.exception.SdmxUnauthorisedException;
import io.sdmx.api.format.FILE_FORMAT;
import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.api.sdmx.engine.DataReaderEngine;
import io.sdmx.api.sdmx.manager.structure.SdmxBeanRetrievalManager;
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
import io.sdmx.utils.core.io.AbstractReadableDataLocation;
import io.sdmx.utils.core.io.SdmxSourceReadableDataLocationFactory;
import io.sdmx.utils.http.api.model.IHttpProxy;
import io.sdmx.utils.http.broker.RestMessageBroker;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.names.SDMXComponentAlias;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
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
	
	// This is a field present only in SDMXMetadataRepository and SDMXJsonMetadataRepository classes 
	private static final Field RBRM; 
	// This is a method present only in SDMXMetadataRepository and SDMXJsonMetadataRepository classes
	private static final Method RESOLVE_DATAFLOW; 

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

		try
		{
			Class<?> sdmxRepoClass = Class.forName("it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository", true, currentThread().getContextClassLoader());
			RBRM = sdmxRepoClass.getField("rbrm");
			RESOLVE_DATAFLOW = sdmxRepoClass.getMethod("resolveDataflow", VTLAlias.class);
		}
		catch (ClassNotFoundException | NoSuchFieldException | SecurityException | NoSuchMethodException e)
		{
			throw new ExceptionInInitializerError(e);
		}
	}

	private static class GzippedRDL extends AbstractReadableDataLocation
	{
		private static final long serialVersionUID = 1L;

		public GzippedRDL(ReadableDataLocation rdl)
		{
			super(rdl);
		}

		@Override
		public String getName()
		{
			return "";
		}
		
		@Override
		public FILE_FORMAT getFormat()
		{
			return super.getFormat();
		}

		@Override
		public ReadableDataLocation copy()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		protected void closeInternal()
		{
		}

		@Override
		protected InputStream getStreamInternal() throws IOException
		{
			return new GZIPInputStream(getProxy().getInputStream());
		}
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
		if (repo == null)
			return Optional.empty();
		
		SDMXComponentAlias query;
		try
		{
			query = (SDMXComponentAlias) RESOLVE_DATAFLOW.invoke(repo, alias);
		}
		catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			throw new IllegalStateException("The SDMX Environment must be used with a SDMX Metadata Repository.");
		}
		if (query == null)
			return Optional.empty();

		Optional<DataSetStructure> maybeStructure = repo.getMetadata(query).map(DataSetStructure.class::cast);
		if (maybeStructure.isEmpty())
			return Optional.empty();
		
		DataSetStructure structure = maybeStructure.get();
		AbstractDataSet sdmxDataflow = new StreamWrapperDataSet(structure, () -> getData(repo, query, structure));
		return Optional.of(sdmxDataflow);
	}

	static SimpleEntry<DateTimeFormatter, TemporalQuery<? extends TemporalAccessor>> getDateParser(String dateStr)
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

	private synchronized Stream<DataPoint> getData(MetadataRepository repo, SDMXComponentAlias dataflow, DataSetStructure structure)
	{
		String path = String.format("%s/data/%s,%s,%s", endpoint, coalesce(dataflow.getMaintainable().getAgency(), "all"), 
			dataflow.getMaintainable().getId().getName(), coalesce(dataflow.getMaintainable().getVersion(), "latest"));
		String resource = dataflow.getComponent().getName();
		if (!resource.isBlank())
			 path += "/" + resource;
		
		ReadableDataLocation rdl;
		try 
		{
			rdl = RDL_FACTORY.getReadableDataLocation(path);
		}
		catch (SdmxUnauthorisedException e)
		{
			if (username != null && password != null && !username.isBlank() && !password.isEmpty())
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
					rdl = RDL_FACTORY.getReadableDataLocation(is);
				}
				catch (IOException | URISyntaxException e1)
				{
					throw new VTLNestedException("Error connecting to " + path, e);
				}
			else
				throw new VTLNestedException("Error connecting to " + path, e);
		}

		SdmxBeanRetrievalManager brm = null;
		try
		{
			brm = (SdmxBeanRetrievalManager) RBRM.get(repo);
		}
		catch (SecurityException | ReflectiveOperationException | IllegalArgumentException e)
		{
			throw new IllegalStateException("The SDMX Environment must be used with a SDMX Metadata Repository.");
		}
		
		if (rdl.getFormat() == FILE_FORMAT.GZIP)
			rdl = new GzippedRDL(rdl);
		
		DataReaderManager manager = new DataReaderManagerImpl(new DataFormatManagerImpl(null, new InformationFormatManager()));
		synchronized (brm) {
			DataReaderEngine dre = manager.getDataReaderEngine(rdl, brm, new FirstFailureErrorHandler());
			return StreamSupport.stream(spliteratorUnknownSize(new ObsIterator(dataflow, dre, structure), IMMUTABLE), SEQUENTIAL);
		}
	}
}
