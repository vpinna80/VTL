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
package it.bancaditalia.oss.vtl.impl.meta.sdmx;

import static io.sdmx.api.sdmx.constants.TEXT_TYPE.STRING;
import static io.sdmx.core.sdmx.util.BeanRetreivalUtil.getEnumeratedRepMap;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getGlobalPropertyValue;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getLocalPropertyValue;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_PASSWORD;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_URL;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME_PERIODDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithKeys;
import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import io.sdmx.api.exception.SdmxNoResultsException;
import io.sdmx.api.sdmx.constants.TEXT_TYPE;
import io.sdmx.api.sdmx.model.beans.base.ComponentBean;
import io.sdmx.api.sdmx.model.beans.base.MaintainableBean;
import io.sdmx.api.sdmx.model.beans.base.RepresentationBean;
import io.sdmx.api.sdmx.model.beans.base.TextFormatBean;
import io.sdmx.api.sdmx.model.beans.codelist.CodelistBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DataStructureBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DataflowBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DimensionBean;
import io.sdmx.api.sdmx.model.beans.datastructure.PrimaryMeasureBean;
import io.sdmx.api.sdmx.model.beans.reference.CrossReferenceBean;
import io.sdmx.api.sdmx.model.beans.reference.MaintainableRefBean;
import io.sdmx.api.sdmx.model.beans.reference.StructureReferenceBean;
import io.sdmx.api.sdmx.model.beans.transformation.ITransformationSchemeBean;
import io.sdmx.core.sdmx.manager.structure.SdmxRestToBeanRetrievalManager;
import io.sdmx.core.sdmx.manager.structure.StructureReaderManagerImpl;
import io.sdmx.format.ml.factory.data.SdmxMLDataReaderFactory;
import io.sdmx.format.ml.factory.structure.SdmxMLStructureReaderFactory;
import io.sdmx.fusion.service.builder.StructureQueryBuilderRest;
import io.sdmx.fusion.service.constant.REST_API_VERSION;
import io.sdmx.fusion.service.engine.RESTQueryBrokerEngineImpl;
import io.sdmx.fusion.service.manager.RESTSdmxBeanRetrievalManager;
import io.sdmx.utils.core.application.SingletonStore;
import io.sdmx.utils.http.api.model.IHttpProxy;
import io.sdmx.utils.http.broker.RestMessageBroker;
import io.sdmx.utils.sdmx.xs.MaintainableRefBeanImpl;
import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.exceptions.VTLAmbiguousAliasException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureDefinitionImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.NonNullDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.RangeIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.RangeNumberDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.RegExpDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.StrlenDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.SDMXAlias;
import it.bancaditalia.oss.vtl.impl.types.names.SDMXComponentAlias;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataStructureDefinition;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class SDMXRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SDMXRepository.class);
	
	private static final RegExpDomainSubset VTL_ALPHA = new RegExpDomainSubset(VTLAliasImpl.of(true, "ALPHA"), "(?U)^\\p{Alpha}*$", STRINGDS);
	private static final RegExpDomainSubset VTL_ALPHA_NUMERIC = new RegExpDomainSubset(VTLAliasImpl.of(true, "ALPHA_NUMERIC"), "(?U)^\\p{Alnum}*$", STRINGDS);
	private static final RegExpDomainSubset VTL_NUMERIC = new RegExpDomainSubset(VTLAliasImpl.of(true, "NUMERIC"), "(?U)^\\p{Digit}*$", STRINGDS);
	private static final Map<String, SDMXRepository> CACHE = new ConcurrentHashMap<>();

	public static final VTLProperty SDMX_REGISTRY_ENDPOINT = new VTLPropertyImpl("vtl.sdmx.meta.endpoint", "SDMX REST metadata base URL", "https://www.myurl.com/service", EnumSet.of(IS_REQUIRED, IS_URL));
	public static final VTLProperty SDMX_API_VERSION = new VTLPropertyImpl("vtl.sdmx.meta.version", "SDMX REST API version", "1.5.0", EnumSet.of(IS_REQUIRED), "1.5.0");
	public static final VTLProperty SDMX_META_USERNAME = new VTLPropertyImpl("vtl.sdmx.meta.user", "SDMX REST user name", "", emptySet());
	public static final VTLProperty SDMX_META_PASSWORD = new VTLPropertyImpl("vtl.sdmx.meta.password", "SDMX REST password", "", EnumSet.of(IS_PASSWORD));

	static
	{
		ConfigurationManager.registerSupportedProperties(SDMXRepository.class, SDMX_REGISTRY_ENDPOINT, SDMX_API_VERSION, SDMX_META_USERNAME, SDMX_META_PASSWORD);

		// sdmx.io singletons initialization
		SingletonStore.registerInstance(new RESTQueryBrokerEngineImpl());
		SingletonStore.registerInstance(new StructureQueryBuilderRest());
		SingletonStore.registerInstance(new StructureReaderManagerImpl());
		SdmxMLStructureReaderFactory.registerInstance();
		SdmxMLDataReaderFactory.registerInstance();
	}
	
	public final SdmxRestToBeanRetrievalManager rbrm;
	
	private final Map<VTLAlias, DataflowBean> dataflows;
	private final Map<SDMXAlias, Entry<DataSetStructure, List<DataSetComponent<Identifier, ?, ?>>>> dsds;
	// four-layered map: id - agency - parentid - version > variable
	private final Map<String, List<Variable<?, ?>>> variables;
	private final Map<VTLAlias, ValueDomainSubset<?, ?>> domains;
	private final Map<VTLAlias, String> schemes;
	private final String endpoint;

	/* Only used to load transformation schemes from a registry */
	public SDMXRepository(boolean ignored) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		this(null, getGlobalPropertyValue(SDMX_REGISTRY_ENDPOINT), getGlobalPropertyValue(SDMX_API_VERSION), getGlobalPropertyValue(SDMX_META_USERNAME), getGlobalPropertyValue(SDMX_META_PASSWORD));
	}

	public SDMXRepository() throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		this(null, getLocalPropertyValue(SDMX_REGISTRY_ENDPOINT), getLocalPropertyValue(SDMX_API_VERSION), getLocalPropertyValue(SDMX_META_USERNAME), getLocalPropertyValue(SDMX_META_PASSWORD));
	}

	public SDMXRepository(String endpoint, String username, String password) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		this(null, endpoint, getLocalPropertyValue(SDMX_API_VERSION), username, password);
	}
	
	public SDMXRepository(MetadataRepository chained, String endpoint, String apiVersion, String username, String password) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		super(chained);
		
		if (endpoint == null || endpoint.isEmpty())
			throw new IllegalStateException("No endpoint configured for SDMX REST service.");
		
		SDMXRepository cached = CACHE.putIfAbsent(endpoint, this);
		
		if (cached != null)
		{
			this.endpoint = cached.endpoint;
			dataflows = cached.dataflows;
			dsds = cached.dsds;
			variables = cached.variables;
			domains = cached.domains;
			schemes = cached.schemes;
			rbrm = cached.rbrm;

			LOGGER.info("SDMX Environment configuration recovered from cache.");
		}
		else
		{
			this.endpoint = endpoint;
			dsds = new HashMap<>();
			variables = new HashMap<>();
			domains = new HashMap<>();
			schemes = new HashMap<>();

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
			
			if (username != null && !username.isEmpty() && password != null && !password.isEmpty())
				RestMessageBroker.storeGlobalAuthorization(username, password);
			
			LOGGER.info("Loading metadata from {}", endpoint);
	
			rbrm = new SdmxRestToBeanRetrievalManager(new RESTSdmxBeanRetrievalManager(endpoint, 
				REST_API_VERSION.parseVersion(apiVersion)));
	
			// Define ALPHA and ALPHA_NUMERIC domains
			domains.put(VTLAliasImpl.of(true, "ALPHA"), VTL_ALPHA);
			domains.put(VTLAliasImpl.of(true, "ALPHA_NUMERIC"), VTL_ALPHA_NUMERIC);
			
			// Load dataflows
			LOGGER.debug("Loading dataflows...");
			dataflows = Utils.getStream(rbrm.getMaintainableBeans(DataflowBean.class))
				.collect(toMapWithKeys(df -> new SDMXAlias(df.getAgencyId(), df.getId(), df.getVersion())));
	
			// Load transformation schemes
			try
			{
				for (ITransformationSchemeBean scheme: rbrm.getIdentifiables(ITransformationSchemeBean.class))
				{
					StructureReferenceBean schemeRef = scheme.asReference();
					VTLAlias tsName = new SDMXAlias(schemeRef.getAgencyId(), schemeRef.getMaintainableId(), schemeRef.getVersion());
					LOGGER.info("Loading transformation scheme {}", tsName);
					String code = scheme.getItems().stream()
						.map(t -> t.getResult() + (t.isPersistent() ? "<-" : ":=") + t.getExpression())
						.collect(joining(";" + lineSeparator() + lineSeparator(), "", ";" + lineSeparator()));
					LOGGER.debug("Loaded transformation scheme {} with code:\n{}\n", tsName, code);
					schemes.put(tsName, code);
				}
				LOGGER.info("Loaded transformation schemes.");
			}
			catch (RuntimeException e)
			{
				if (e.getCause() instanceof SdmxNoResultsException)
					LOGGER.info("No VTL transformation schemes found in SDMX registry.");
				else
					LOGGER.error("Error accessing transformation schemes in registry: {}", e.getMessage());
			}
			
			LOGGER.info("SDMX Environment initialization complete.");
		}
	}
	
	@Override
	public Optional<ValueDomainSubset<?, ?>> getDomain(VTLAlias alias)
	{
		return Optional.<ValueDomainSubset<?, ?>>ofNullable(domains.get(alias))
			.or(() -> {
				try
				{
					SDMXAlias sdmx = (SDMXAlias) alias;
					MaintainableRefBean clRef = new MaintainableRefBeanImpl(sdmx.getAgency(), sdmx.getId().getName(), sdmx.getVersion());
					CodelistBean codelist = fetchCodelist(endpoint, clRef);
					SdmxCodeList domain = new SdmxCodeList(endpoint, codelist); 
					domains.put(domain.getAlias(), domain);
					return Optional.of(domain);
				}
				catch (Exception e)
				{
					LOGGER.debug("Could not fetch SDMX codelist {}", alias);
					LOGGER.trace("Could not fetch SDMX codelist " + alias + ": " + e.getMessage(), e);
					return Optional.empty();
				}
			}).or(() -> super.getDomain(alias)); 
	}
	
	@Override
	public Optional<HierarchicalRuleSet> getHierarchyRuleset(VTLAlias alias)
	{
		return getDomain(alias)
			.filter(SdmxCodeList.class::isInstance)
			.map(d -> ((SdmxCodeList) d).getDefaultRuleSet())
			.or(() -> super.getHierarchyRuleset(alias));
	}
	
	@Override
	public Optional<DataPointRuleSet> getDataPointRuleset(VTLAlias alias)
	{
		return super.getDataPointRuleset(alias);
	}
	
	@Override
	public Optional<DataStructureDefinition> getStructureDefinition(VTLAlias alias)
	{
		if (alias instanceof SDMXAlias)
			return Optional.ofNullable(dsds.get(alias))
				.map(dsd -> dsd.getKey().stream()
						.map(c -> new DataStructureComponentImpl<>(c.getAlias(), c.getRole()))
						.collect(collectingAndThen(toSet(), comps -> (DataStructureDefinition) new DataStructureDefinitionImpl(alias, comps)))
				).or(() -> super.getStructureDefinition(alias));
		else
			return super.getStructureDefinition(alias);
	}
	
	@Override
	public Optional<VTLValueMetadata> getMetadata(VTLAlias alias)
	{
		SDMXComponentAlias query = resolveDataflow(alias);
		if (query != null && dataflows.containsKey(query.getMaintainable()))
		{
			SDMXAlias dataflow = query.getMaintainable();
			LOGGER.info("Found SDMX dataflow matching " + query);
			CrossReferenceBean strRefBean = dataflows.get(dataflow).getDataStructureRef();
			SDMXAlias strAlias = new SDMXAlias(strRefBean.getAgencyId(), strRefBean.getMaintainableId(), strRefBean.getVersion());
			if (!dsds.containsKey(strAlias))
				populateDSD(strRefBean);
			Entry<DataSetStructure, List<DataSetComponent<Identifier, ?, ?>>> structureEntry = dsds.get(strAlias);
			DataSetStructure structure = structureEntry.getKey();
			
			// drop identifiers in the query part of the id
			String cube = query.getComponent().getName();
			if (cube != null && !cube.isBlank())
			{
				List<DataSetComponent<Identifier, ?, ?>> subbedIDs = structureEntry.getValue();
				DataSetStructureBuilder builder = new DataSetStructureBuilder(structure);
				
				String[] dims = cube.split("\\.", -1);
				for (int i = 0; i < dims.length; i++)
					if (!dims[i].isEmpty() && dims[i].indexOf('+') < 0)
						builder.removeComponent(subbedIDs.get(i));
				
				structure = builder.build();
			}
			
			return Optional.of(structure);
		}
		else
			return super.getMetadata(alias);
	}
	
	private void populateDSD(CrossReferenceBean strRefBean)
	{
		SDMXAlias dsdName = new SDMXAlias(strRefBean.getAgencyId(), strRefBean.getMaintainableId(), strRefBean.getVersion());
		LOGGER.debug("Loading structure {}", dsdName);
		
		DataStructureBean dsd;
		Map<ComponentBean, CrossReferenceBean> clRefs;
		synchronized (rbrm)
		{
			dsd = rbrm.getMaintainableBean(DataStructureBean.class, strRefBean);
			clRefs = getEnumeratedRepMap(rbrm, dsd);
		}

		DataSetStructureBuilder builder = new DataSetStructureBuilder();
		
		// Get all codelist refs used in the structure, either from it or the list of concepts 
		
		Map<Integer, DataSetComponent<Identifier, ?, ?>> enumIds = new TreeMap<>();
		for (List<? extends ComponentBean> compsOfType: List.of(dsd.getDimensions(), dsd.getMeasures(), dsd.getAttributes()))
			for (ComponentBean compBean: compsOfType)
			{
				ValueDomainSubset<?, ?> domain;

				if (compBean instanceof DimensionBean && ((DimensionBean) compBean).isTimeDimension())
					domain = TIMEDS;
				else if (compBean instanceof PrimaryMeasureBean)
					domain = NUMBERDS;
				else
				{
					CrossReferenceBean clRef = clRefs.get(compBean);
					if (clRef != null)
					{
						SDMXAlias alias = new SDMXAlias(clRef.getAgencyId(), clRef.getMaintainableId(), clRef.getVersion());
						domain = getDomain(alias)
							.or(() -> Optional.of(domains.computeIfAbsent(alias, a -> new SdmxCodeList(endpoint, clRef, alias))))
							.orElseThrow(() -> new VTLUndefinedObjectException("Domain", alias));
					}
					else
						domain = sdmxRepr2VTLDomain(compBean);
				}
				
				Class<? extends Component> role;
				switch (compBean.getType())
				{
					case ATTRIBUTE: role = Attribute.class; break;
					case DIMENSION: role = Identifier.class; break;
					case MEASURE: role = Measure.class; break;
					default: continue;
				}
				
				DataSetComponent<?, ?, ?> comp = createComponent(compBean, role, domain);
				if (compBean instanceof DimensionBean)
					enumIds.put(((DimensionBean) compBean).getPosition() - 1, comp.asRole(Identifier.class));
				builder.addComponent(comp);
				LOGGER.trace("Created {} {} from DSD {}", comp.getRole().getSimpleName(), dsdName, comp);
			}

		DataSetStructure structure = builder.build();
		dsds.put(dsdName, new SimpleEntry<>(structure, new ArrayList<>(enumIds.values())));
		LOGGER.debug("Created datastructure {} as {}", dsdName, structure);
	}

	@Override
	public Optional<Variable<?, ?>> getVariable(VTLAlias alias)
	{
		String agency;
		String parentID;
		String version;
		String component;
		
		if (alias instanceof SDMXComponentAlias)
		{
			SDMXComponentAlias compAlias = (SDMXComponentAlias) alias;
			SDMXAlias maint = compAlias.getMaintainable();
			agency = maint.getAgency();
			parentID = maint.getId().getName().toString();
			version = maint.getVersion();
			component = compAlias.getComponent().getName().toString();
		}
		else if (alias instanceof SDMXAlias)
		{
			SDMXAlias compAlias = (SDMXAlias) alias;
			agency = null;
			parentID = compAlias.getAgency();
			version = null;
			component = compAlias.getId().getName().toString();
		}
		else
		{
			agency = null;
			parentID = null;
			version = null;
			component = alias.getName();
		}
		
		Stream<Variable<?, ?>> stream = variables.get(component).stream();
		if (agency != null)
			stream = stream.filter(v -> agency.equals(((SDMXComponentAlias) v.getAlias()).getMaintainable().getAgency()));
		if (parentID != null)
			stream = stream.filter(v -> parentID.equals(((SDMXComponentAlias) v.getAlias()).getMaintainable().getId().getName()));
		if (version != null)
			stream = stream.filter(v -> version.equals(((SDMXComponentAlias) v.getAlias()).getMaintainable().getVersion()));
		
		List<Variable<?, ?>> vars = stream.collect(toList());
		
		if (vars.isEmpty())
			return super.getVariable(alias);
		else if (vars.size() > 1)
			throw new VTLAmbiguousAliasException(alias, vars.stream().map(Variable::getAlias).collect(toList()));
		
		return Optional.of(vars.get(0));
	}

	@Override
	public Optional<String> getTransformationScheme(VTLAlias alias)
	{
		return Optional.ofNullable(schemes.get(alias));
	}

	@Override
	public Set<VTLAlias> getAvailableSchemeAliases()
	{
		return schemes.keySet();
	}
	
	public SDMXComponentAlias resolveDataflow(VTLAlias alias)
	{
		if (alias instanceof SDMXAlias)
		{
			SDMXAlias sdmx = (SDMXAlias) alias;
			DataflowBean df = dataflows.get(sdmx);
			if (df != null)
				return new SDMXComponentAlias(df.getAgencyId(), df.getId(), df.getVersion(), "");
			else if (sdmx.getVersion() == null)
				return resolveDataflow(new SDMXComponentAlias(null, sdmx.getAgency(), null, sdmx.getId().getName()));
			else
				return null;
		}
		else if (alias instanceof SDMXComponentAlias)
		{
			SDMXComponentAlias sdmx = (SDMXComponentAlias) alias;
			DataflowBean df = dataflows.get(sdmx.getMaintainable());
			
			if (df != null)
				return new SDMXComponentAlias(df.getAgencyId(), df.getId(), df.getVersion(), sdmx.getComponent().getName());
			else
				return null;
		}
		else if (alias instanceof VTLAlias)
			return resolveDataflow(new SDMXComponentAlias(null, alias.getName(), null, ""));
		else
			return null;
	}

	private static ValueDomainSubset<?, ?> sdmxRepr2VTLDomain(ComponentBean compBean)
	{
		Optional<TextFormatBean> optFormat = Optional.of(compBean)
				.map(ComponentBean::getRepresentation)
				.map(RepresentationBean::getTextFormat);
		
		TEXT_TYPE type = optFormat
				.map(TextFormatBean::getTextType)
				.orElse(STRING);
		
		ValueDomainSubset<?, ?> domain;
		boolean inclusive = true;
		
		switch (type)
		{
			case ALPHA: domain = VTL_ALPHA; break;
			case ALPHA_NUMERIC: domain = VTL_ALPHA_NUMERIC; break;
			case NUMERIC: domain = VTL_NUMERIC; break;
			case BOOLEAN: domain = BOOLEANDS; break;
			case STRING: case URI: domain = STRINGDS; break;
			case INCLUSIVE_VALUE_RANGE: domain = NUMBERDS; break;
			case EXCLUSIVE_VALUE_RANGE: domain = NUMBERDS; inclusive = false; break;
			case FLOAT: case DOUBLE: case DECIMAL: case INCREMENTAL: domain = NUMBERDS; break;
			case BIG_INTEGER: case COUNT: case LONG: case INTEGER: case SHORT: domain = INTEGERDS; break;
			case DURATION: domain = DURATIONDS; break;
			case OBSERVATIONAL_TIME_PERIOD: case STANDARD_TIME_PERIOD: case TIME_RANGE: domain = TIMEDS; break; 
			case DATE_TIME: case BASIC_TIME_PERIOD: case GREGORIAN_TIME_PERIOD: case GREGORIAN_DAY: case GREGORIAN_YEAR: case GREGORIAN_YEAR_MONTH: domain = DATEDS; break;
			case MONTH: case MONTH_DAY: case DAY: case TIME: domain = STRINGDS; break;
			case REPORTING_DAY: case REPORTING_MONTH: case REPORTING_QUARTER: case REPORTING_SEMESTER:
			case REPORTING_TIME_PERIOD: case REPORTING_TRIMESTER: case REPORTING_WEEK: case REPORTING_YEAR: domain = TIME_PERIODDS; break; 
			case GEO: case XHTML: default: LOGGER.warn("Representation {} is not implemented, String will be used instead.", type); domain = STRINGDS; break;
		}
		
		if (optFormat.isPresent())
		{
			TextFormatBean format = optFormat.get();
			if (STRINGDS.isAssignableFrom(domain))
			{
				OptionalInt minLen = Stream.ofNullable(format.getMinLength()).mapToInt(BigInteger::intValueExact).findAny();
				OptionalInt maxLen = Stream.ofNullable(format.getMaxLength()).mapToInt(BigInteger::intValueExact).findAny();
				domain = new StrlenDomainSubset(STRINGDS, minLen, maxLen);
			}
			else if (domain instanceof IntegerDomain)
			{
				OptionalLong minLen = Stream.ofNullable(format.getMinValue()).mapToLong(BigDecimal::longValueExact).findAny();
				OptionalLong maxLen = Stream.ofNullable(format.getMaxValue()).mapToLong(BigDecimal::longValueExact).findAny();
				VTLAlias name = VTLAliasImpl.of(true, domain.getAlias() + ">=" + minLen.orElse(Long.MIN_VALUE) + "<" + maxLen.orElse(Long.MAX_VALUE));
				domain = new RangeIntegerDomainSubset<>(name, INTEGERDS, minLen, maxLen, inclusive);
			}
			else if (NUMBERDS.isAssignableFrom(domain))
			{
				OptionalDouble minLen = Stream.ofNullable(format.getMinValue()).mapToDouble(BigDecimal::doubleValue).findAny();
				OptionalDouble maxLen = Stream.ofNullable(format.getMaxValue()).mapToDouble(BigDecimal::doubleValue).findAny();
				VTLAlias name = VTLAliasImpl.of(true, domain.getAlias() + ">=" + minLen.orElse(Long.MIN_VALUE) + "<" + maxLen.orElse(Long.MAX_VALUE));
				domain = new RangeNumberDomainSubset<>(name, NUMBERDS, minLen, maxLen, inclusive);
			}
			
			if (compBean instanceof DimensionBean || compBean.isMandatory())
			{
				@SuppressWarnings({ "rawtypes", "unchecked" })
				NonNullDomainSubset<?, ?> nonNullDomainSubset = new NonNullDomainSubset(domain);
				domain = nonNullDomainSubset;
			}
		}
		
		return domain;
	}
	
	private <R extends Component> DataSetComponent<R, ?, ?> createComponent(ComponentBean bean, Class<R> role, ValueDomainSubset<?, ?> domain)
	{
		MaintainableBean strRef = bean.getMaintainableParent();
		VTLAlias compAlias = new SDMXComponentAlias(new SDMXAlias(strRef.getAgencyId(), strRef.getId(), strRef.getVersion()), bean.getId());
		
		variables.computeIfAbsent(bean.getId(), al -> new ArrayList<>()).add(VariableImpl.of(compAlias, domain));
		DataSetComponent<R, ?, ?> component = DataSetComponentImpl.of(compAlias, domain, role);
		
		String sdmxType = bean.getClass().getSimpleName();
		sdmxType = sdmxType.substring(0, sdmxType.length() - 8);
		LOGGER.trace("{} {} converted to component {}", sdmxType, compAlias, component);
		
		return component;
	}

	public static CodelistBean fetchCodelist(String endpoint, MaintainableRefBean clRef)
	{
		SDMXRepository instance = CACHE.get(endpoint);
		synchronized (instance.rbrm) {
			try
			{
				return instance.rbrm.getMaintainableBean(CodelistBean.class, clRef);
			}
			catch (RuntimeException e)
			{
				throw new VTLNestedException("Could not fetch SDMX codelist " + clRef, e);
			}
		}
	}
}
