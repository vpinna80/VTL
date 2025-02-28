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
package it.bancaditalia.oss.vtl.impl.meta.json;

import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder.toDataStructure;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.tcds.TransformationCriterionDomainSubset.TEST_ALIAS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.json.JsonMapper;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLDuplicatedObjectException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.domain.tcds.StringTransformationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.tcds.TransformationCriterionScope;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerBiFunction;

public class JsonMetadataRepository extends InMemoryMetadataRepository
{
	public static final VTLProperty JSON_METADATA_URL = new VTLPropertyImpl("vtl.json.metadata.url", "Json url providing structures and domains", "file://", EnumSet.of(IS_REQUIRED));

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonMetadataRepository.class);
	private static final Map<String, Class<? extends Component>> ROLES = new HashMap<>(); 
	
	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(JsonMetadataRepository.class, JSON_METADATA_URL);
		
		ROLES.put("Identifier", Identifier.class);
		ROLES.put("Measure", Measure.class);
		ROLES.put("Attribute", Attribute.class);
		ROLES.put("Viral Attribute", ViralAttribute.class);
	}

	private final Map<VTLAlias, Variable<?, ?>> variables = new HashMap<>(); 
	private final Map<VTLAlias, String> sources = new HashMap<>();
	private final Map<VTLAlias, ValueDomainSubset<?, ?>> domains = new HashMap<>(); 
	// dsd alias to dsd
	private final Map<VTLAlias, VTLValueMetadata> structures = new HashMap<>();
	// data alias to dsd alias
	private final Map<VTLAlias, VTLAlias> data = new HashMap<>(); 
	
	public JsonMetadataRepository() throws IOException
	{
		this(new URL(JSON_METADATA_URL.getValue()));
	}
	
	public JsonMetadataRepository(MetadataRepository chained) throws IOException
	{
		this(chained, new URL(JSON_METADATA_URL.getValue()));
	}
	
	public JsonMetadataRepository(URL jsonURL) throws IOException
	{
		this(null, jsonURL, ConfigurationManagerFactory.newManager().getEngine());
	}
	
	public JsonMetadataRepository(MetadataRepository chained, URL jsonURL) throws IOException
	{
		this(chained, jsonURL, ConfigurationManagerFactory.newManager().getEngine());
	}

	public JsonMetadataRepository(URL jsonURL, Engine engine) throws IOException
	{
		this(null, jsonURL, engine);
	}

	public JsonMetadataRepository(MetadataRepository chained, URL jsonURL, Engine engine) throws IOException
	{
		super(chained);
		
		Map<VTLAlias, Entry<VTLAlias, String>> dsDefs;
		Map<VTLAlias, Map<VTLAlias, Class<? extends Component>>> strDefs;
		Map<VTLAlias, VTLAlias> varDefs;
		
		try (InputStream source = jsonURL.openStream())
		{
			Map<?, ?> json;
			try (JsonParser parser = JsonFactory.builder().build().setCodec(new JsonMapper()).createParser(source))
			{
				json = parser.readValueAs(Map.class);
			}
			
			iterate(json, "domain", (a, d) -> createDomain(a, d, requireNonNull(engine)));
			varDefs = iterate(json, "variable", JsonMetadataRepository::createVariable);
			strDefs = iterate(json, "structure", JsonMetadataRepository::createMetadata);
			dsDefs = iterate(json, "dataset", JsonMetadataRepository::createData);
		}

		varDefs.forEach((alias, d) -> variables.put(alias, VariableImpl.of(alias, getDomain(d).orElseThrow(() -> new VTLUndefinedObjectException("Domain", d)))));
		strDefs.forEach((dsdAlias, l) -> structures.put(dsdAlias, l.entrySet().stream()
				.map(splitting((vAlias, r) -> getVariable(vAlias).orElseThrow(() -> new VTLUndefinedObjectException("Variable", vAlias)).as(r)))
				.collect(toDataStructure())));
		dsDefs.forEach((alias, e) -> {
			data.put(alias, e.getKey());
			sources.put(alias, e.getValue());
			if (getStructureDefinition(e.getKey()).isEmpty())
				structures.put(e.getKey(), ScalarValueMetadata.of(getDomain(e.getKey()).orElseThrow(() -> new VTLUndefinedObjectException("Structure or domain", e.getKey()))));
		});
		
	}
	
	@Override
	public Optional<VTLValueMetadata> getMetadata(VTLAlias alias)
	{
		return Optional.ofNullable(data.get(alias))
				.flatMap(this::getStructureDefinition)
				.or(() -> super.getMetadata(alias));
	}
	
	@Override
	public Optional<VTLValueMetadata> getStructureDefinition(VTLAlias alias)
	{
		return Optional.ofNullable(structures.get(alias)).or(() -> super.getStructureDefinition(alias));
	}
	
	@Override
	public Optional<Variable<?, ?>> getVariable(VTLAlias alias)
	{
		return Optional.<Variable<?, ?>>ofNullable(variables.get(alias)).or(() -> super.getVariable(alias));
	}
	
	@Override
	public Optional<ValueDomainSubset<?, ?>> getDomain(VTLAlias alias)
	{
		return Optional.<ValueDomainSubset<?, ?>>ofNullable(domains.get(alias)).or(() -> super.getDomain(alias));
	}
	
	@Override
	public String getDataSource(VTLAlias name)
	{
		String source = sources.get(name);
		
		return source != null ? source : super.getDataSource(name);
	}
	
	private static <T> Map<VTLAlias, T> iterate(Map<?, ?> json, String element, SerBiFunction<VTLAlias, Map<?, ?>, T> processor)
	{
		Map<VTLAlias, T> result = new HashMap<>();
		List<?> list = (List<?>) json.get(element + "s");
		
		if (list != null)
			for (Object entry: list)
				if (entry instanceof Map)
				{
					Map<?, ?> obj = (Map<?, ?>) entry;
					if (!obj.containsKey("name"))
						throw new IllegalStateException("missing name for object");
					if (!(obj.get("name") instanceof String))
						throw new IllegalStateException("object name is not a string");
					
					VTLAlias name = VTLAliasImpl.of((String) obj.get("name"));
					if (result.containsKey(name))
						throw new VTLDuplicatedObjectException(element, name);
					T processed = processor.apply(name, obj);
					if (processed != null)
						result.put(name, processed);
				}
				else
					throw new InvalidParameterException("Expected JSON object but found " + entry.getClass());
		
		return result;
	}
	
	private static Entry<VTLAlias, String> createData(VTLAlias name, Map<?, ?> dataset)
	{
		VTLAlias metadataRef = VTLAliasImpl.of((String) dataset.get("structure"));
		String source = (String) dataset.get("source");
		
		LOGGER.debug("Found dataset {} with structure {}", name, metadataRef);
		return new SimpleEntry<>(metadataRef, source);
	}

	private static Map<VTLAlias, Class<? extends Component>> createMetadata(VTLAlias name, Map<?, ?> jsonStructure)
	{
		LOGGER.info("Found structure {}", name);
		return iterate(jsonStructure, "component", JsonMetadataRepository::createComponent);
	}

	private static Class<? extends Component> createComponent(VTLAlias name, Map<?, ?> jsonStructure)
	{
		return ROLES.get(jsonStructure.get("role"));
	}

	private static VTLAlias createVariable(VTLAlias name, Map<?, ?> variable)
	{
		return VTLAliasImpl.of((String) variable.get("domain"));
	}
	
	private ValueDomainSubset<?, ?> createDomain(VTLAlias name, Map<?, ?> domainDef, Engine engine)
	{
		Object parent = ((Map<?, ?>) domainDef).get("parent");
		if (parent == null || !(parent instanceof String))
			throw new InvalidParameterException("Parent domain invalid or not specified for " + name + ".");

		LOGGER.debug("Found domain {}", name);
		Object enumerated = domainDef.get("enumerated");
		Object described = domainDef.get("described");
		if (enumerated instanceof List)
			if ("string".equals(parent))
			{
				Set<String> codes = ((List<?>) enumerated).stream().map(String.class::cast).collect(toSet());
				LOGGER.debug("Obtained {} codes for {}", codes.size(), name);
				StringCodeList domain = new StringCodeList(STRINGDS, name, codes);
				domains.put(name, domain);
				return domain;
			}
			else
				LOGGER.warn("Ignoring unsupported domain {}[{}].", name, parent);
		else if (enumerated != null)
			throw new InvalidParameterException("Invalid enumerated domain definition for " + name + ".");
		else if (described instanceof String)
		{
			if ("string".equals(parent))
			{
				String code = (String) described;
				
				List<Statement> statements;
				try
				{
					statements = engine.parseRules(TEST_ALIAS + " := " + code + ";").collect(toList());
				}
				catch (RuntimeException e)
				{
					throw new VTLNestedException("Syntax error defining domain " + name, e);
				}
				
				if (statements.size() != 1)
					throw new InvalidParameterException("Invalid domain definition expression: " + code);
				
				Statement statement = statements.get(0);
				if (!(statement instanceof Transformation))
					throw new InvalidParameterException("Invalid domain definition expression: " + code);
				
				VTLValueMetadata meta = ((Transformation) statement).getMetadata(new TransformationCriterionScope<>(STRINGDS));
				if (!(!meta.isDataSet()))
					throw new InvalidParameterException("Invalid domain definition expression: " + code);
				
				if (!(((ScalarValueMetadata<?, ?>) meta).getDomain() instanceof BooleanDomainSubset))
					throw new InvalidParameterException("Invalid domain definition expression: " + code);
				
				ValueDomainSubset<?, ?> domain = new StringTransformationDomainSubset<>(name, STRINGDS, (Transformation) statement);
				domains.put(name, domain);
				return domain;
			}
			else
				LOGGER.warn("Ignoring unsupported domain {}[{}].", name, parent);
		}
		else
			LOGGER.warn("Ignoring unsupported domain type {}[{}].", name, parent);
		
		return null;
	}
}
