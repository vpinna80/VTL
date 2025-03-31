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
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_PASSWORD;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME_PERIODDS;
import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import io.sdmx.api.exception.SdmxNoResultsException;
import io.sdmx.api.sdmx.constants.TEXT_TYPE;
import io.sdmx.api.sdmx.model.beans.base.ComponentBean;
import io.sdmx.api.sdmx.model.beans.base.RepresentationBean;
import io.sdmx.api.sdmx.model.beans.base.TextFormatBean;
import io.sdmx.api.sdmx.model.beans.codelist.CodelistBean;
import io.sdmx.api.sdmx.model.beans.datastructure.AttributeBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DataStructureBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DataflowBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DimensionBean;
import io.sdmx.api.sdmx.model.beans.datastructure.PrimaryMeasureBean;
import io.sdmx.api.sdmx.model.beans.reference.StructureReferenceBean;
import io.sdmx.api.sdmx.model.beans.transformation.ITransformationSchemeBean;
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
import io.sdmx.utils.sdmx.xs.MaintainableRefBeanImpl;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.NonNullDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.RangeIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.RangeNumberDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.RegExpDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.StrlenDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class SDMXRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SDMXRepository.class);
	private static final Pattern SDMX_DATAFLOW_PATTERN = Pattern.compile("^([[\\p{Alnum}][_.]]+:[[\\p{Alnum}][_.]]+\\([0-9._+*~]+\\))(?:/(.*))?$");
	private static final RegExpDomainSubset VTL_ALPHA = new RegExpDomainSubset(VTLAliasImpl.of(true, "ALPHA"), "(?U)^\\p{Alpha}*$", STRINGDS);
	private static final RegExpDomainSubset VTL_ALPHA_NUMERIC = new RegExpDomainSubset(VTLAliasImpl.of(true, "ALPHA_NUMERIC"), "(?U)^\\p{Alnum}*$", STRINGDS);
	private static final RegExpDomainSubset VTL_NUMERIC = new RegExpDomainSubset(VTLAliasImpl.of(true, "NUMERIC"), "(?U)^\\p{Digit}*$", STRINGDS);

	public static final VTLProperty SDMX_REGISTRY_ENDPOINT = new VTLPropertyImpl("vtl.sdmx.meta.endpoint", "SDMX REST metadata base URL", "https://www.myurl.com/service", EnumSet.of(IS_REQUIRED));
	public static final VTLProperty SDMX_API_VERSION = new VTLPropertyImpl("vtl.sdmx.meta.version", "SDMX REST API version", "1.5.0", EnumSet.of(IS_REQUIRED), "1.5.0");
	public static final VTLProperty SDMX_META_USERNAME = new VTLPropertyImpl("vtl.sdmx.meta.user", "SDMX REST user name", "", emptySet());
	public static final VTLProperty SDMX_META_PASSWORD = new VTLPropertyImpl("vtl.sdmx.meta.password", "SDMX REST password", "", EnumSet.of(IS_PASSWORD));

	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(SDMXRepository.class, SDMX_REGISTRY_ENDPOINT, SDMX_API_VERSION, SDMX_META_USERNAME, SDMX_META_PASSWORD);

		// sdmx.io singletons initialization
		SingletonStore.registerInstance(new RESTQueryBrokerEngineImpl());
		SingletonStore.registerInstance(new StructureQueryBuilderRest());
		SingletonStore.registerInstance(new StructureReaderManagerImpl());
		SdmxMLStructureReaderFactory.registerInstance();
	}

	public final SdmxRestToBeanRetrievalManager rbrm;
	
	private final Map<VTLAlias, Entry<VTLAlias, List<DataStructureComponent<Identifier, ?, ?>>>> dataflows = new HashMap<>();
	private final Map<VTLAlias, VTLValueMetadata> dsds = new HashMap<>();
	private final Map<VTLAlias, Map<String, Variable<?, ?>>> variables = new HashMap<>();
	private final Map<VTLAlias, String> schemes = new HashMap<>();
	private final Map<VTLAlias, ValueDomainSubset<?, ?>> domains = new HashMap<>();

	public SDMXRepository() throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		this(null, SDMX_REGISTRY_ENDPOINT.getValue(), SDMX_META_USERNAME.getValue(), SDMX_META_PASSWORD.getValue());
	}
	
	public SDMXRepository(MetadataRepository chained) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		this(chained, SDMX_REGISTRY_ENDPOINT.getValue(), SDMX_META_USERNAME.getValue(), SDMX_META_PASSWORD.getValue());
	}

	public SDMXRepository(String endpoint, String username, String password) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		this(null, endpoint, username, password);
	}
	
	public SDMXRepository(MetadataRepository chained, String endpoint, String username, String password) throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		super(chained);
		
		if (endpoint == null || endpoint.isEmpty())
			throw new IllegalStateException("No endpoint configured for SDMX REST service.");

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

		rbrm = new SdmxRestToBeanRetrievalManager(new RESTSdmxBeanRetrievalManager(endpoint, REST_API_VERSION.parseVersion(SDMX_API_VERSION.getValue())));
		
		// Load codelists
		for (CodelistBean codelist: rbrm.getMaintainableBeans(CodelistBean.class, new MaintainableRefBeanImpl(null, null, "*")))
		{
			VTLAlias clName = sdmxRef2VtlName(codelist.asReference());
			LOGGER.info("Loading codelist " + clName);
			domains.put(clName, new SdmxCodeList(codelist));
		}

		// Define ALPHA and ALPHA_NUMERIC domains
		domains.put(VTLAliasImpl.of(true, "ALPHA"), VTL_ALPHA);
		domains.put(VTLAliasImpl.of(true, "ALPHA_NUMERIC"), VTL_ALPHA_NUMERIC);
		
		// Load dsds
		Map<VTLAlias, List<DataStructureComponent<Identifier, ?, ?>>> enumIDMap = new HashMap<>();
		for (DataStructureBean dsd: rbrm.getIdentifiables(DataStructureBean.class))
		{
			DataStructureBuilder builder = new DataStructureBuilder();
			VTLAlias dsdName = sdmxRef2VtlName(dsd.asReference());
			LOGGER.info("Loading structure {}", dsdName);
			Map<Integer, DataStructureComponent<Identifier, ?, ?>> enumIds = new TreeMap<>();
			
			for (DimensionBean dimBean: dsd.getDimensionList().getDimensions())
			{
				ValueDomainSubset<?, ?> domain;

				if (dimBean.isTimeDimension())
					domain = TIMEDS;
				else if (dimBean.hasCodedRepresentation())
				{
					VTLAlias alias = sdmxRef2VtlName(dimBean.getEnumeratedRepresentation());
					domain = getDomain(alias).orElseThrow(() -> new VTLUndefinedObjectException("Domain", alias));
				}
				else
					domain = sdmxRepr2VTLDomain(dimBean);

				DataStructureComponent<Identifier, ?, ?> id = createComponent(dimBean, Identifier.class, domain);
				LOGGER.debug("From dsd {} created identifier {}", dsdName, id);
				builder.addComponent(id);
				enumIds.put(dimBean.getPosition() - 1, id);
			}

			builder.addComponent(createObsValue(dsd.getPrimaryMeasure()));
			
			for (AttributeBean attrBean: dsd.getAttributeList().getAttributes())
			{
				ValueDomainSubset<?, ?> domain;
				
				if (attrBean.hasCodedRepresentation())
				{
					VTLAlias alias = sdmxRef2VtlName(attrBean.getEnumeratedRepresentation());
					domain = getDomain(alias).orElseThrow(() -> new VTLUndefinedObjectException("Domain", alias));
				}
				else
					domain = sdmxRepr2VTLDomain(attrBean);
				
				DataStructureComponent<Attribute, ?, ?> attr = createComponent(attrBean, Attribute.class, domain);
				LOGGER.debug("From dsd {} created attribute {}", dsdName, attr);
				builder.addComponent(attr);
			}

			dsds.put(dsdName, builder.build());
			enumIDMap.put(dsdName, new ArrayList<>(enumIds.values()));
		}
		
		// Load dataflows
		for (DataflowBean dataflow: rbrm.getIdentifiables(DataflowBean.class))
		{
			VTLAlias dataflowName = sdmxRef2VtlName(dataflow.asReference());
			VTLAlias dsdName = sdmxRef2VtlName(dataflow.getDataStructureRef());
			LOGGER.info("Loading dataflow {} with structure {}", dataflowName, dsdName);
			dataflows.put(dataflowName, new SimpleEntry<>(dsdName, enumIDMap.get(dsdName)));
		}
		
		// Load transformation schemes
		try
		{
			for (ITransformationSchemeBean scheme: rbrm.getIdentifiables(ITransformationSchemeBean.class))
			{
				VTLAlias tsName = sdmxRef2VtlName(scheme.asReference());
				LOGGER.info("Loading transformation scheme {}", tsName);
				String code = scheme.getItems().stream()
					.map(t -> t.getResult() + (t.isPersistent() ? "<-" : ":=") + t.getExpression())
					.collect(joining(";" + lineSeparator() + lineSeparator(), "", ";" + lineSeparator()));
				LOGGER.debug("Loaded transformation scheme {} with code:\n{}\n", tsName, code);
				schemes.put(tsName, code);
			}
		}
		catch (RuntimeException e)
		{
			if (e.getCause() instanceof SdmxNoResultsException)
				LOGGER.warn("No transformation schemes found.");
			else
				LOGGER.error("Error accessing transformation schemes in registry", e);
		}
	}
	
	@Override
	public Optional<ValueDomainSubset<?, ?>> getDomain(VTLAlias alias)
	{
		return Optional.<ValueDomainSubset<?, ?>>ofNullable(domains.get(alias)).or(() -> super.getDomain(alias)); 
	}
	
	@Override
	public HierarchicalRuleSet<?, ?, ?, ?> getHierarchyRuleset(VTLAlias alias)
	{
		return getDomain(alias)
				.filter(SdmxCodeList.class::isInstance)
				.map(SdmxCodeList.class::cast)
				.map(SdmxCodeList::getDefaultRuleSet)
				.map(HierarchicalRuleSet.class::cast)
				.orElseGet(() -> super.getHierarchyRuleset(alias));
	}
	
	@Override
	public DataPointRuleSet getDataPointRuleset(VTLAlias alias)
	{
		return super.getDataPointRuleset(alias);
	}
	
	@Override
	public Optional<VTLValueMetadata> getStructureDefinition(VTLAlias alias)
	{
		return Optional.ofNullable(dsds.get(alias)).or(() -> super.getStructureDefinition(alias));
	}
	
	@Override
	public Optional<VTLValueMetadata> getMetadata(VTLAlias alias)
	{
		Matcher matcher = SDMX_DATAFLOW_PATTERN.matcher(alias.getName());
		if (matcher.matches() && dataflows.containsKey(VTLAliasImpl.of(true, matcher.group(1))))
		{
			Entry<VTLAlias, List<DataStructureComponent<Identifier, ?, ?>>> metadata = dataflows.get(VTLAliasImpl.of(true, matcher.group(1)));
			DataSetMetadata structure = (DataSetMetadata) getStructureDefinition(metadata.getKey()).get();
			List<DataStructureComponent<Identifier, ?, ?>> subbedIDs = metadata.getValue();
			// drop identifiers in the query part of the id
			if (matcher.group(2) != null)
			{
				DataStructureBuilder builder = new DataStructureBuilder(structure);
				
				String[] dims = matcher.group(2).split("\\.");
				for (int i = 0; i < dims.length; i++)
					if (!dims[i].isEmpty() && dims[i].indexOf('+') <= 0)
						builder.removeComponent(subbedIDs.get(i));
				
				structure = builder.build();
			}
			
			return Optional.of(structure);
		}
		else
			return super.getMetadata(alias);
	}
	
	@Override
	public Optional<Variable<?, ?>> getVariable(VTLAlias alias)
	{
		String agency = alias.toString().indexOf(":") >= 0 ? alias.toString().split(":", 2)[0] : null;
		
		Variable<?, ?> variable = null;
		Map<String, Variable<?, ?>> varsOfConcept = variables.get(alias);
		if (agency != null)
			variable = varsOfConcept.get(agency);
		else if (varsOfConcept != null && !varsOfConcept.isEmpty())
			variable = varsOfConcept.values().iterator().next();
		
		return Optional.<Variable<?, ?>>ofNullable(variable).or(() -> super.getVariable(alias));
	}
	
	public TransformationScheme getTransformationScheme(VTLAlias alias)
	{
		return Optional.ofNullable(schemes.get(alias))
			.map(ConfigurationManagerFactory.newManager()::createSession)
			.orElseThrow(() -> new VTLException("Transformation scheme " + alias + " not found."));
	}
	
	public Set<VTLAlias> getAvailableSchemes()
	{
		return schemes.keySet();
	}

	private static VTLAlias sdmxRef2VtlName(StructureReferenceBean ref)
	{
		return VTLAliasImpl.of(true, ref.getAgencyId() + ":" + ref.getMaintainableId() + "(" + ref.getVersion() + ")");
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
				domain = new StrlenDomainSubset<>(STRINGDS, minLen, maxLen);
			}
			else if (INTEGERDS.isAssignableFrom(domain))
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
	
	private DataStructureComponent<Measure, ?, ?> createObsValue(PrimaryMeasureBean obs_value)
	{
		VTLAlias alias = VTLAliasImpl.of(true, obs_value.getId());
		return variables.computeIfAbsent(alias, id -> new HashMap<>())
			.computeIfAbsent(obs_value.getMaintainableParent().getAgencyId(), ag -> VariableImpl.of(alias, NUMBERDS))
			.as(Measure.class);
	}

	private <R extends Component> DataStructureComponent<R, ?, ?> createComponent(ComponentBean bean, Class<R> role, ValueDomainSubset<?, ?> domain)
	{
		VTLAlias name = VTLAliasImpl.of(true, bean.getId());
		
		variables.putIfAbsent(name, new HashMap<>());
		Variable<?, ?> variable = variables.get(name).compute(bean.getConceptRef().getAgencyId(), (ag, v) -> VariableImpl.of(name, domain));
		DataStructureComponent<R, ?, ?> component = variable.as(role);
		
		String sdmxType = bean.getClass().getSimpleName();
		sdmxType = sdmxType.substring(0, sdmxType.length() - 8);
		LOGGER.debug("{} {} converted to component {}", sdmxType, name, component);
		
		return component;
	}
}
