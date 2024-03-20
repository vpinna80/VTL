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
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.PASSWORD;
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.REQUIRED;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyMap;
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

import io.sdmx.api.sdmx.constants.TEXT_TYPE;
import io.sdmx.api.sdmx.manager.structure.SdmxBeanRetrievalManager;
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
import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.RangeIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.RegExpDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.StrlenDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class SDMXRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SDMXRepository.class);
	private static final Pattern SDMX_DATAFLOW_PATTERN = Pattern.compile("^([[\\p{Alnum}][_.]]+:[[\\p{Alnum}][_.]]+\\([0-9._+*~]+\\))(?:/(.*))?$");
	private static final RegExpDomainSubset VTL_ALPHA = new RegExpDomainSubset("ALPHA", "(?U)^\\p{Alpha}*$", STRINGDS);
	private static final RegExpDomainSubset VTL_ALPHA_NUMERIC = new RegExpDomainSubset("ALPHA_NUMERIC", "(?U)^\\p{Alnum}*$", STRINGDS);

	public static final VTLProperty SDMX_REGISTRY_ENDPOINT = new VTLPropertyImpl("vtl.sdmx.meta.endpoint", "SDMX REST metadata base URL", "https://www.myurl.com/service", EnumSet.of(REQUIRED));
	public static final VTLProperty SDMX_API_VERSION = new VTLPropertyImpl("vtl.sdmx.meta.version", "SDMX REST API version", "1.5.0", EnumSet.of(REQUIRED), "1.5.0");
	public static final VTLProperty SDMX_META_USERNAME = new VTLPropertyImpl("vtl.sdmx.meta.user", "SDMX REST user name", "", EnumSet.noneOf(Flags.class));
	public static final VTLProperty SDMX_META_PASSWORD = new VTLPropertyImpl("vtl.sdmx.meta.password", "SDMX REST password", "", EnumSet.of(PASSWORD));

	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(SDMXRepository.class, SDMX_REGISTRY_ENDPOINT, SDMX_API_VERSION, SDMX_META_USERNAME, SDMX_META_PASSWORD);

		// sdmx.io singletons initialization
		SingletonStore.registerInstance(new RESTQueryBrokerEngineImpl());
		SingletonStore.registerInstance(new StructureQueryBuilderRest());
		SingletonStore.registerInstance(new StructureReaderManagerImpl());
		SdmxMLStructureReaderFactory.registerInstance();
	}

	private final String url = SDMX_REGISTRY_ENDPOINT.getValue();
	private final Map<String, Entry<DataSetMetadata, List<DataStructureComponent<Identifier, ?, ?>>>> dataflows = new HashMap<>();
	private final Map<String, Map<String, Variable<?, ?>>> variables = new HashMap<>();
	private final Map<String, String> schemes = new HashMap<>();
	private final SdmxRestToBeanRetrievalManager rbrm;

	public SDMXRepository() throws IOException, SAXException, ParserConfigurationException, URISyntaxException
	{
		if (url == null || url.isEmpty())
			throw new IllegalStateException("No endpoint configured for SDMX REST service.");

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
		
		String userName = SDMX_META_USERNAME.getValue();
		String password = SDMX_META_PASSWORD.getValue();
		if (userName != null && !userName.isEmpty() && password != null && !password.isEmpty())
			RestMessageBroker.storeGlobalAuthorization(userName, password);
		
		LOGGER.info("Loading metadata from {}", url);

		rbrm = new SdmxRestToBeanRetrievalManager(new RESTSdmxBeanRetrievalManager(url, REST_API_VERSION.parseVersion(SDMX_API_VERSION.getValue())));
		
		// Load codelists
		for (CodelistBean codelist: rbrm.getMaintainableBeans(CodelistBean.class, new MaintainableRefBeanImpl(null, null, "*")))
		{
			final String clName = sdmxRef2VtlName(codelist.asReference());
			LOGGER.info("Loading codelist " + clName);
			defineDomain(clName, new SdmxCodeList(codelist));
		}

		// Define ALPHA and ALPHA_NUMERIC domains
		defineDomain("ALPHA", VTL_ALPHA);
		defineDomain("ALPHA_NUMERIC", VTL_ALPHA_NUMERIC);
		
		// Load structures
		Map<String, Entry<DataSetMetadata, List<DataStructureComponent<Identifier, ?, ?>>>> structures = new HashMap<>();
		for (DataStructureBean dsd: rbrm.getIdentifiables(DataStructureBean.class))
		{
			DataStructureBuilder builder = new DataStructureBuilder();
			String dsdName = sdmxRef2VtlName(dsd.asReference());
			LOGGER.info("Loading structure {}", dsdName);
			Map<Integer, DataStructureComponent<Identifier, ?, ?>> enumIds = new TreeMap<>();
			
			for (DimensionBean dimBean: dsd.getDimensionList().getDimensions())
			{
				ValueDomainSubset<?, ?> domain = dimBean.isTimeDimension() ? TIMEDS : getDomain(sdmxRef2VtlName(dimBean.getEnumeratedRepresentation()));
				DataStructureComponent<Identifier, ?, ?> id = createComponent(dimBean, Identifier.class, domain);
				builder.addComponent(id);
				enumIds.put(dimBean.getPosition() - 1, id);
			}

			builder.addComponent(createObsValue(dsd.getPrimaryMeasure()));
			
			for (AttributeBean attrBean: dsd.getAttributeList().getAttributes())
			{
				ValueDomainSubset<?, ?> domain;
				
				if (attrBean.hasCodedRepresentation())
					domain = getDomain(sdmxRef2VtlName(attrBean.getEnumeratedRepresentation()));
				else
				{
					Optional<TextFormatBean> optFormat = Optional.of(attrBean)
							.map(AttributeBean::getRepresentation)
							.map(RepresentationBean::getTextFormat);
					
					TextFormatBean format = optFormat.orElse(null);
					TEXT_TYPE type = optFormat
							.map(TextFormatBean::getTextType)
							.orElse(STRING);
					
					switch (type)
					{
						// TODO: Implement more SDMX representations
						// BASIC_TIME_PERIOD, DATE_TIME, DAY, DURATION, EXCLUSIVE_VALUE_RANGE, GEO, GREGORIAN_DAY, GREGORIAN_TIME_PERIOD, 
						// GREGORIAN_YEAR, GREGORIAN_YEAR_MONTH, INCLUSIVE_VALUE_RANGE, INCREMENTAL, MONTH, MONTH_DAY, OBSERVATIONAL_TIME_PERIOD, REPORTING_DAY,
						// REPORTING_MONTH, REPORTING_QUARTER, REPORTING_SEMESTER, REPORTING_TIME_PERIOD, REPORTING_TRIMESTER, REPORTING_WEEK, REPORTING_YEAR, 
						// STANDARD_TIME_PERIOD, TIME, TIME_RANGE, URI, XHTML
						case ALPHA: domain = VTL_ALPHA; break;
						case ALPHA_NUMERIC: domain = VTL_ALPHA_NUMERIC; break;
						case BOOLEAN: domain = BOOLEANDS; break;
						case STRING: domain = STRINGDS; break;
						case FLOAT: case DOUBLE: case NUMERIC: case DECIMAL: domain = NUMBERDS; break;
						case BIG_INTEGER: case COUNT: case LONG: case INTEGER: case SHORT: domain = INTEGERDS; break;
						default: LOGGER.warn("Representation {} is not implemented, String will be used instead.", type); domain = STRINGDS;
					}
					
					if (format != null)
					{
						if (STRINGDS.isAssignableFrom(domain))
						{
							OptionalInt minLen = Stream.ofNullable(format.getMinLength()).mapToInt(BigInteger::intValueExact).findAny();
							OptionalInt maxLen = Stream.ofNullable(format.getMaxLength()).mapToInt(BigInteger::intValueExact).findAny();
							domain = new StrlenDomainSubset<>(STRINGDS, minLen, maxLen);
						}
						if (INTEGERDS.isAssignableFrom(domain))
						{
							OptionalLong minLen = Stream.ofNullable(format.getMinValue()).mapToLong(BigDecimal::longValueExact).findAny();
							OptionalLong maxLen = Stream.ofNullable(format.getMaxValue()).mapToLong(BigDecimal::longValueExact).findAny();
							String name = domain.getName() + ">=" + minLen.orElse(Long.MIN_VALUE) + "<" + maxLen.orElse(Long.MAX_VALUE);
							domain = new RangeIntegerDomainSubset<>(name, INTEGERDS, minLen, maxLen);
						}
					}
				}
				
				builder.addComponent(createComponent(attrBean, Attribute.class, domain));
			}

			structures.put(dsdName, new SimpleEntry<>(builder.build(), new ArrayList<>(enumIds.values())));
		}
		
		// Load dataflows
		for (DataflowBean dataflow: rbrm.getIdentifiables(DataflowBean.class))
		{
			String dataflowName = sdmxRef2VtlName(dataflow.asReference());
			String dsdName = sdmxRef2VtlName(dataflow.getDataStructureRef());
			LOGGER.info("Loading dataflow {} with structure {}", dataflowName, dsdName);
			dataflows.put(dataflowName, structures.get(dsdName));
		}
		
		// Load transformation schemes
		for (ITransformationSchemeBean scheme: rbrm.getIdentifiables(ITransformationSchemeBean.class))
		{
			String code = scheme.getItems().stream()
				.map(t -> t.getResult() + (t.isPersistent() ? "<-" : ":=") + t.getExpression())
				.collect(joining(";" + lineSeparator() + lineSeparator(), "", ";" + lineSeparator()));
			schemes.put(sdmxRef2VtlName(scheme.asReference()), code);
		}
	}
	
	private DataStructureComponent<Measure, ?, ?> createObsValue(PrimaryMeasureBean obs_value)
	{
		String alias = obs_value.getId();
		return variables.computeIfAbsent(obs_value.getId(), id -> new HashMap<>())
			.computeIfAbsent(obs_value.getMaintainableParent().getAgencyId(), ag -> VariableImpl.of(alias, NUMBERDS))
			.getComponent(Measure.class);
	}

	private <R extends Component> DataStructureComponent<R, ?, ?> createComponent(ComponentBean bean, Class<R> role, ValueDomainSubset<?, ?> domain)
	{
		String name = bean.getId();
		
		variables.putIfAbsent(name, new HashMap<>());
		Variable<?, ?> variable = variables.get(name).compute(bean.getConceptRef().getAgencyId(), (ag, v) -> VariableImpl.of(name, domain));
		DataStructureComponent<R, ?, ?> component = variable.getComponent(role);
		
		String sdmxType = bean.getClass().getSimpleName();
		sdmxType = sdmxType.substring(0, sdmxType.length() - 8);
		LOGGER.debug("{} {} converted to component {}", sdmxType, name, component);
		
		return component;
	}
	
	@Override
	public StringHierarchicalRuleSet getHierarchyRuleset(String alias)
	{
		return maybeGetDomain(alias)
				.filter(SdmxCodeList.class::isInstance)
				.map(SdmxCodeList.class::cast)
				.map(SdmxCodeList::getDefaultRuleSet)
				.orElseThrow(() -> new VTLException("Hierarchical ruleset " + alias + " not found."));
	}
	
	@Override
	public DataSetMetadata getStructure(String alias)
	{
		Matcher matcher = SDMX_DATAFLOW_PATTERN.matcher(alias);
		if (matcher.matches() && dataflows.containsKey(matcher.group(1)))
		{
			Entry<DataSetMetadata, List<DataStructureComponent<Identifier, ?, ?>>> entry = dataflows.get(matcher.group(1));
			DataSetMetadata structure = entry.getKey();
			// drop identifiers in the query part of the id
			if (matcher.group(2) != null)
			{
				DataStructureBuilder builder = new DataStructureBuilder(structure);
				
				String[] dims = matcher.group(2).split("\\.");
				for (int i = 0; i < dims.length; i++)
					if (!dims[i].isEmpty() && dims[i].indexOf('+') <= 0)
						builder.removeComponent(entry.getValue().get(i));
				
				structure = builder.build();
			}
			
			return structure;
		}
		else
			return super.getStructure(alias);
	}
	
	private static String sdmxRef2VtlName(StructureReferenceBean ref)
	{
		return ref.getAgencyId() + ":" + ref.getMaintainableId() + "(" + ref.getVersion() + ")";
	}
	
	@Override
	public <S extends ValueDomainSubset<S, D>, D extends ValueDomain> Variable<S, D> getVariable(String alias, ValueDomainSubset<S, D> domain)
	{
		String agency = domain instanceof SdmxCodeList ? ((SdmxCodeList) domain).getAgency() : null;
		
		Variable<?, ?> variable = null;
		Map<String, Variable<?, ?>> varsOfConcept = variables.get(alias);
		if (agency != null)
			variable = varsOfConcept.get(agency);
		else if (varsOfConcept != null && !varsOfConcept.isEmpty())
			variable = varsOfConcept.values().iterator().next();
		
		if (variable == null)
			variable = super.getVariable(alias, domain);
		
		if (variable != null)
		{
			@SuppressWarnings("unchecked")
			Variable<S, D> ret = (Variable<S, D>) variable;
			return ret;
		}
		else
			throw new VTLException("Variable " + alias + " not found.");
	}

	public SdmxBeanRetrievalManager getBeanRetrievalManager()
	{
		return rbrm;
	}
	
	@Override
	public TransformationScheme getTransformationScheme(String alias)
	{
		return Optional.ofNullable(schemes.get(alias))
			.map(ConfigurationManager.getDefault()::createSession)
			.orElseThrow(() -> new VTLException("Transformation scheme " + alias + " not found."));
	}
	
	public Set<String> getAvailableSchemes()
	{
		return schemes.keySet();
	}
}
