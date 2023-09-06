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
package it.bancaditalia.oss.vtl.impl.meta.fmr;

import static io.sdmx.api.sdmx.constants.SDMX_STRUCTURE_TYPE.CODE_LIST;
import static io.sdmx.api.sdmx.constants.SDMX_STRUCTURE_TYPE.DATAFLOW;
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.PASSWORD;
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.REQUIRED;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import io.sdmx.api.exception.SdmxNoResultsException;
import io.sdmx.api.sdmx.constants.SDMX_STRUCTURE_TYPE;
import io.sdmx.api.sdmx.model.beans.datastructure.AttributeBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DataStructureBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DataflowBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DimensionBean;
import io.sdmx.api.sdmx.model.beans.reference.CrossReferenceBean;
import io.sdmx.api.sdmx.model.beans.reference.StructureReferenceBean;
import io.sdmx.core.sdmx.manager.structure.SdmxRestToBeanRetrievalManager;
import io.sdmx.core.sdmx.manager.structure.StructureReaderManagerImpl;
import io.sdmx.format.ml.factory.structure.SdmxMLStructureReaderFactory;
import io.sdmx.fusion.service.builder.StructureQueryBuilderRest;
import io.sdmx.fusion.service.constant.REST_API_VERSION;
import io.sdmx.fusion.service.engine.RESTQueryBrokerEngineImpl;
import io.sdmx.fusion.service.manager.RESTSdmxBeanRetrievalManager;
import io.sdmx.utils.core.application.SingletonStore;
import io.sdmx.utils.http.api.model.IHttpProxy;
import io.sdmx.utils.http.broker.RestMessageBroker;
import io.sdmx.utils.sdmx.xs.StructureReferenceBeanImpl;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class FMRRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(FMRRepository.class);

	public static final VTLProperty FM_REGISTRY_ENDPOINT = new VTLPropertyImpl("vtl.fmr.endpoint", "Fusion Metadata Registry base URL", "https://www.myurl.com/service", EnumSet.of(REQUIRED));
	public static final VTLProperty FMR_API_VERSION = new VTLPropertyImpl("vtl.fmr.version", "Fusion Metadata Registry Rest API version", "1.5.0", EnumSet.of(REQUIRED), "1.5.0");
	public static final VTLProperty FMR_USERNAME = new VTLPropertyImpl("vtl.fmr.global.user", "Fusion Metadata Registry user name", "", EnumSet.noneOf(Flags.class));
	public static final VTLProperty FMR_PASSWORD = new VTLPropertyImpl("vtl.fmr.global.password", "Fusion Metadata Registry password", "", EnumSet.of(PASSWORD));
	public static final VTLProperty SDMX_ENVIRONMENT_AUTODROP_IDENTIFIERS = new VTLPropertyImpl("vtl.sdmx.keep.identifiers", "True to keep subspaced identifiers", "false", EnumSet.noneOf(Flags.class), "false");
	public static final Pattern SDMX_PATTERN = Pattern.compile("^([[\\p{Alnum}][_.]]+):([[\\p{Alnum}][_.]]+)(?:\\(([0-9._+*~]+)\\))(?:/.*)?$");
	
	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(FMRRepository.class, FM_REGISTRY_ENDPOINT, FMR_API_VERSION, FMR_USERNAME, FMR_PASSWORD, SDMX_ENVIRONMENT_AUTODROP_IDENTIFIERS);
	}

	private final String url = FM_REGISTRY_ENDPOINT.getValue();
	private final boolean drop = Boolean.parseBoolean(SDMX_ENVIRONMENT_AUTODROP_IDENTIFIERS.getValue());
	
	private transient SdmxRestToBeanRetrievalManager rbrm;

	public FMRRepository() throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		if (url == null || url.isEmpty())
			throw new IllegalStateException("No endpoint configured for FMR repository.");

		// FMR client configuration
		SingletonStore.registerInstance(new RESTQueryBrokerEngineImpl());
		SingletonStore.registerInstance(new StructureQueryBuilderRest());
		SingletonStore.registerInstance(new StructureReaderManagerImpl());
		SdmxMLStructureReaderFactory.registerInstance();

		URI uri = new URI(url);
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
		
		String userName = FMR_USERNAME.getValue();
		String password = FMR_PASSWORD.getValue();
		if (userName != null && !userName.isEmpty() && password != null && !password.isEmpty())
			RestMessageBroker.storeGlobalAuthorization(userName, password);
		
		LOGGER.info("Loading metadata from {}", url);
	}
	
	@Override
	public ValueDomainSubset<?, ?> getDomain(String alias)
	{
		Optional<ValueDomainSubset<?, ?>> maybeDomain = maybeGetDomain(alias);
		if (maybeDomain.isPresent())
			return maybeDomain.get();
		
		StructureReferenceBean refBean = vtlName2SdmxRef(alias, CODE_LIST);
		if (refBean != null)
			LOGGER.info("Found codelist {} in Fusion Metadata Registry", alias);
		return refBean != null ? defineDomain(alias, new LazyCodeList(STRINGDS, refBean, this)) : super.getDomain(alias);
	}
	
	@Override
	public DataSetMetadata getStructure(String alias)
	{
		StructureReferenceBean ref = vtlName2SdmxRef(alias, DATAFLOW);
		if (ref == null)
		{
			LOGGER.debug("{} is not an SDMX dataflow reference.", alias);
			return super.getStructure(alias);
		}

		String[] dims;
		if (drop && alias.indexOf('/') > 0)
			dims = alias.split("/", 2)[1].split("\\.");
		else
			dims = new String[] {};
		
		int iDim = 0;
		try
		{
			// SDMX to VTL mapping
			SdmxRestToBeanRetrievalManager rbrm = getBeanRetrievalManager();
						
			DataflowBean sourceBean = rbrm.getIdentifiableBean(ref, DataflowBean.class);
			if (sourceBean == null)
				throw new VTLException("Cannot find SDMX dataflow with " + ref);
			CrossReferenceBean dsdRef = sourceBean.getDataStructureRef();
			DataStructureBean dsd = rbrm.getIdentifiableBean(dsdRef, DataStructureBean.class);
			DataStructureBuilder builder = new DataStructureBuilder();
			
			for (DimensionBean dimBean: dsd.getDimensionList().getDimensions())
				if (!drop || iDim >= dims.length || dims[iDim++].isEmpty())
					builder.addComponent(DataStructureComponentImpl.of('\'' + dimBean.getId() + '\'', Identifier.class, 
							(ValueDomainSubset<?, ?>) (dimBean.isTimeDimension() ? TIMEDS : getDomain(sdmxRef2VtlName(dimBean.getEnumeratedRepresentation())))));

			builder.addComponent(DataStructureComponentImpl.of('\'' + dsd.getPrimaryMeasure().getId() + '\'', Measure.class, NUMBERDS));
			// Support for multiple measures (SDMX 3.0)
//			for (MeasureDimensionBean measureBean: dsd.getMeasures())
//				builder.addComponent(DataStructureComponentImpl.of(measureBean.getId(), Measure.class, NUMBERDS));
			
			for (AttributeBean attrBean: dsd.getAttributeList().getAttributes())
				builder.addComponent(DataStructureComponentImpl.of('\'' + attrBean.getId() + '\'', Attribute.class, STRINGDS));

			return builder.build();
		}
		catch (SdmxNoResultsException e)
		{
			return super.getStructure(alias);
		}
	}

	public boolean isDrop()
	{
		return drop;
	}

	public SdmxRestToBeanRetrievalManager getBeanRetrievalManager()
	{
		if (rbrm == null)
			synchronized (this) 
			{
				if (rbrm == null)
					rbrm = new SdmxRestToBeanRetrievalManager(new RESTSdmxBeanRetrievalManager(url, REST_API_VERSION.parseVersion(FMR_API_VERSION.getValue())));
			}
		
		return rbrm;
	}
	
	private StructureReferenceBean vtlName2SdmxRef(String alias, SDMX_STRUCTURE_TYPE type)
	{
		Matcher matcher = SDMX_PATTERN.matcher(alias);
		if (matcher.matches())
		{
			String agencyId = matcher.group(1); 
			String maintainableId = matcher.group(2); 
			String version = matcher.group(3);
			return new StructureReferenceBeanImpl(agencyId, maintainableId, version, type);
		}
		else
			return null;
	}
	
	private String sdmxRef2VtlName(StructureReferenceBean ref)
	{
		return ref.getAgencyId() + ":" + ref.getMaintainableId() + "(" + ref.getVersion() + ")";
	}
}
