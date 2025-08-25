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

import static com.fasterxml.jackson.core.JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION;
import static com.github.erosb.jsonsKema.FormatValidationPolicy.ALWAYS;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getLocalConfigurationObject;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getLocalPropertyValues;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_MULTIPLE;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_URL;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder.toDataStructure;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.lang.System.lineSeparator;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.erosb.jsonsKema.IJsonValue;
import com.github.erosb.jsonsKema.Schema;
import com.github.erosb.jsonsKema.SchemaLoader;
import com.github.erosb.jsonsKema.ValidationFailure;
import com.github.erosb.jsonsKema.Validator;
import com.github.erosb.jsonsKema.ValidatorConfig;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.VTLConfiguration;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureDefinitionImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.domain.tcds.IntegerTransformationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.tcds.NumberTransformationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.tcds.StringTransformationDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.tcds.TransformationCriterionScope;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.DataStructureDefinition;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerBiFunction;

public class JsonMetadataRepository extends InMemoryMetadataRepository
{
	public static final VTLProperty JSON_METADATA_URL = new VTLPropertyImpl("vtl.json.metadata.url", "Json url providing structures and domains", "file://", EnumSet.of(IS_REQUIRED, IS_URL, IS_MULTIPLE));

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonMetadataRepository.class);
	private static final Map<String, Class<? extends Component>> ROLES = new HashMap<>(); 
	
	static
	{
		ConfigurationManager.registerSupportedProperties(JsonMetadataRepository.class, JSON_METADATA_URL);
		
		ROLES.put("Identifier", Identifier.class);
		ROLES.put("Measure", Measure.class);
		ROLES.put("Attribute", Attribute.class);
		ROLES.put("Viral Attribute", ViralAttribute.class);
	}

	private final Map<VTLAlias, ValueDomainSubset<?, ?>> domains; 
	private final Map<VTLAlias, Variable<?, ?>> variables; 
	private final Map<VTLAlias, DataStructureDefinition> structures;
	private final Map<VTLAlias, VTLValueMetadata> data; 
	private final Map<VTLAlias, String> sources = new HashMap<>();
	
	public JsonMetadataRepository() throws IOException
	{
		this(getLocalPropertyValues(JSON_METADATA_URL));
	}
	
	public JsonMetadataRepository(MetadataRepository chained) throws IOException
	{
		this(chained, getLocalPropertyValues(JSON_METADATA_URL));
	}
	
	public JsonMetadataRepository(List<String> jsonURLs) throws IOException
	{
		this(null, jsonURLs, getLocalConfigurationObject(VTLConfiguration::getEngine));
	}
	
	public JsonMetadataRepository(MetadataRepository chained, List<String> jsonURLs) throws IOException
	{
		this(chained, jsonURLs, getLocalConfigurationObject(VTLConfiguration::getEngine));
	}

	public JsonMetadataRepository(List<String> jsonURLs, Engine engine) throws IOException
	{
		this(null, jsonURLs, engine);
	}

	public JsonMetadataRepository(MetadataRepository chained, URL jsonURL, Engine engine) throws IOException
	{
		this(chained, List.of(jsonURL.toString()), engine);
	}
	
	public JsonMetadataRepository(MetadataRepository chained, List<String> jsonURLs, Engine engine) throws IOException
	{
		super(chained);

		try (InputStream schemaIn = JsonMetadataRepository.class.getResourceAsStream("vtl-dict-schema.json"))
		{
			IJsonValue schemaJson = new com.github.erosb.jsonsKema.JsonParser(schemaIn).parse();
			Schema schema = new SchemaLoader(schemaJson).load();
			Validator validator = Validator.create(schema, new ValidatorConfig(ALWAYS));
			
			for (String jsonURL: jsonURLs)
				try (InputStream instanceIn = new URL(jsonURL).openStream())
				{
					IJsonValue instanceJson = new com.github.erosb.jsonsKema.JsonParser(instanceIn).parse();
					ValidationFailure failure = validator.validate(instanceJson);
					if (failure != null)
					{
						Map<?, ?> json = new MappingJsonFactory().createParser(failure.toJSON().toString()).readValueAs(Map.class);
						throw new IllegalStateException("Json validation failed:" + formatFailure(json));
					}
				}
		}

		List<Map<String, Object>> gatheredDomains = new ArrayList<>();
		List<Map<String, Object>> gatheredVariables = new ArrayList<>();
		List<Map<String, Object>> gatheredStructures = new ArrayList<>();
		List<Map<String, Object>> gatheredData = new ArrayList<>();
		
		for (String jsonURL: jsonURLs)
			try (InputStream source = new URL(jsonURL).openStream(); JsonParser parser = new MappingJsonFactory().createParser(source))
			{
				TypeReference<List<Map<String, Object>>> typeRef = new TypeReference<List<Map<String, Object>>>() {};
				ObjectNode root = parser.readValueAsTree();
				
				if (root.has("domains"))
					gatheredDomains.addAll(coalesce(root.get("domains").traverse(parser.getCodec()).readValueAs(typeRef), List.of()));
				if (root.has("variables"))
					gatheredVariables.addAll(coalesce(root.get("variables").traverse(parser.getCodec()).readValueAs(typeRef), List.of()));
				if (root.has("structures"))
					gatheredStructures.addAll(coalesce(root.get("structures").traverse(parser.getCodec()).readValueAs(typeRef), List.of()));
				if (root.has("data"))
					gatheredData.addAll(coalesce(root.get("data").traverse(parser.getCodec()).readValueAs(typeRef), List.of()));
			}
			catch (JsonParseException e)
			{
				try (InputStream source = new URL(jsonURL).openStream(); JsonParser parser = new MappingJsonFactory().enable(INCLUDE_SOURCE_IN_LOCATION).createParser(source))
				{
					parser.readValueAs(Map.class);
					throw e;
				}
				catch (JsonParseException e1)
				{
					throw e1;
				}
			}
		
		try
		{
			// Domains entries must be set inside createDomain due to the recursive nature of domains.
			domains = new HashMap<>();
			iterate(gatheredDomains, "domain", (a, d) -> createDomain(a, d, requireNonNull(engine)));
			variables = iterate(gatheredVariables, "variable", this::createVariable);
			structures = iterate(gatheredStructures, "structure", JsonMetadataRepository::createStructure);
			data = iterate(gatheredData, "data", this::createData);
		}
		catch (VTLException e)
		{
			String lines = Stream.iterate(e, Throwable::getCause).takeWhile(Objects::nonNull).map(Throwable::getMessage).collect(joining(lineSeparator()));
			throw new VTLException("An error occurred while initializing JsonMetadataRepository:\n" + lines);
		}
	}

	private static String formatFailure(Map<?, ?> failure)
	{
		return formatFailure(new StringBuilder(), failure, "\t\t").toString();
	}

	private static StringBuilder formatFailure(StringBuilder sb, Map<?, ?> failure, String indent)
	{
		String loc = ((String) failure.get("instanceRef")).substring(1);
		sb = sb.append("\n").append(indent).append("- In ").append(loc).append(": ").append(failure.get("message"));

	    Object causes = failure.get("causes");
	    if (causes instanceof Iterable)
	        for (Object cause : (Iterable<?>) causes)
	            if (cause instanceof Map)
	            	sb = formatFailure(sb, (Map<?, ?>) cause, indent + "\t");

	    return sb;
	}
	
	@Override
	public Optional<VTLValueMetadata> getMetadata(VTLAlias alias)
	{
		return Optional.ofNullable(data.get(alias))
				.or(() -> super.getMetadata(alias));
	}
	
	@Override
	public Optional<DataStructureDefinition> getStructureDefinition(VTLAlias alias)
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
	public String getDataSource(VTLAlias alias)
	{
		String source = sources.get(alias);
		
		return source != null ? source : super.getDataSource(alias);
	}
	
	private <T> Map<VTLAlias, T> iterate(List<Map<String, Object>> items, String element, SerBiFunction<VTLAlias, Map<String, Object>, T> processor)
	{
		Map<VTLAlias, T> result = new HashMap<>();
		
		for (Map<String, Object> entry: items)
		{
			VTLAlias alias = VTLAliasImpl.of((String) entry.get("name"));
			T processed = processor.apply(alias, entry);
			if (processed != null && result.putIfAbsent(alias, processed) != null)
				LOGGER.warn("Replaced definition of {} {}", element, alias);
		}
		
		return result;
	}
	
	private VTLValueMetadata createData(VTLAlias alias, Map<String, Object> data)
	{
		if (data.containsKey("source"))
			sources.put(alias, (String) data.get("source"));
		
		if (data.containsKey("structure"))
		{
			LOGGER.debug("Found dataset {}", alias);
			VTLAlias strAlias = VTLAliasImpl.of((String) data.get("structure"));
			DataStructureDefinition dsd = getStructureDefinition(strAlias)
				.orElseThrow(() -> new VTLUndefinedObjectException("Structure", strAlias));

			Map<VTLAlias, ValueDomainSubset<?, ?>> defs = coalesce((List<?>) data.get("components"), List.of()).stream()
				.map(o -> (Map<?, ?>) o)
				.map(m -> new SimpleEntry<>(requireNonNull(m.get("name")), requireNonNull(m.get("subset"))))
				.map(e -> new SimpleEntry<>(VTLAliasImpl.of((String) e.getKey()), VTLAliasImpl.of((String) e.getValue())))
				.peek(e -> dsd.getComponent(e.getKey()).orElseThrow(() -> new VTLMissingComponentsException(dsd, e.getKey())))
				.map(keepingKey(v -> getDomain(v).orElseThrow(() -> new VTLUndefinedObjectException("Domain", v))))
				.collect(entriesToMap());
			
			return dsd.stream().map(c -> {
					Optional<ValueDomainSubset<?, ?>> domain = getVariable(c.getAlias()).map(Variable::getDomain);
					ValueDomainSubset<?, ?> subset = defs.get(c.getAlias());
					DataSetComponent<?, ?, ?> component;
					if (subset != null && domain.isPresent())
					{
						if (!domain.get().isAssignableFrom(subset))
							throw new VTLIncompatibleTypesException("Json definition", domain.get(), subset);
						component = DataSetComponentImpl.of(c.getAlias(), subset, c.getRole());
					}
					else if (subset != null && domain.isEmpty())
						component = DataSetComponentImpl.of(c.getAlias(), subset, c.getRole());
					else if (subset == null && domain.isPresent())
						component = DataSetComponentImpl.of(c.getAlias(), domain.get(), c.getRole());
					else
						throw new VTLUndefinedObjectException("Domain or variable for component", c.getAlias());
					return component;
				}).collect(toDataStructure());
		}
		else
		{
			LOGGER.debug("Found scalar {}", alias);
			VTLAlias subset = VTLAliasImpl.of((String) data.get("subset"));
			return getDomain(subset)
				.map(ScalarValueMetadata::of)
				.orElseThrow(() -> new VTLUndefinedObjectException("Domain", subset));
		}
	}

	private static DataStructureDefinition createStructure(VTLAlias alias, Map<String, Object> structure)
	{
		LOGGER.debug("Found structure {}", alias);
		Set<DataStructureComponent<?>> comps = ((List<?>) structure.get("components")).stream()
			.map(j -> (Map<?, ?>) j)
			.map(j -> new SimpleEntry<>((String) j.get("name"), Optional.ofNullable(ROLES.get((String) j.get("role")))
					.orElseThrow(() -> new VTLUndefinedObjectException("Role", VTLAliasImpl.of(true, (String) j.get("role"))))
			)).map(splitting((a, r) -> new DataStructureComponentImpl<>(VTLAliasImpl.of(a), r)))
			.collect(Collectors.toSet());
		
		return new DataStructureDefinitionImpl(alias, comps);
	}

	private Variable<?, ?> createVariable(VTLAlias alias, Map<String, Object> variable)
	{
		VTLAlias domain = VTLAliasImpl.of(true, (String) variable.get("domain"));
		return VariableImpl.of(alias, getDomain(domain).orElseThrow(() -> new VTLUndefinedObjectException("Domain", domain)));
	}
	
	private ValueDomainSubset<?, ?> createDomain(VTLAlias alias, Map<String, Object> domainDef, Engine engine)
	{
		Object parent = ((Map<?, ?>) domainDef).get("parent");
		if (parent == null || !(parent instanceof String))
			throw new InvalidParameterException("Parent domain invalid or not specified for " + alias + ".");
		ValueDomainSubset<?, ?> parentDomain = getDomain(VTLAliasImpl.of((String) parent)).get();

		if (domains.containsKey(alias))
			return domains.get(alias);

		LOGGER.debug("Found domain {}", alias);
		Object enumerated = domainDef.get("enumerated");
		Object described = domainDef.get("described");
		
		if (enumerated instanceof List)
			if (parentDomain instanceof StringDomainSubset)
			{
				Set<String> codes = ((List<?>) enumerated).stream()
					.map(code -> code instanceof String ? (String) code : (String) ((Map<?, ?>) code).get("name"))
					.collect(toSet());
				if (parentDomain instanceof StringCodeList)
					codes.forEach(c -> ((StringCodeList) parentDomain).getCodeItem(c));
				LOGGER.debug("Obtained {} codes for {}", codes.size(), alias);
				
				domains.put(alias, new StringCodeList((StringDomainSubset<?>) parentDomain, alias, codes));
			}
			else
				LOGGER.warn("Ignoring unsupported domain {}[{}].", alias, parentDomain);
		else if (enumerated != null)
			throw new InvalidParameterException("Invalid enumerated domain definition for " + alias + ".");
		else if (described instanceof String)
		{
			String code = (String) described;
			
			List<Statement> statements;
			try
			{
				statements = engine.parseRules("described_domain_test := " + code + ";").collect(toList());
			}
			catch (RuntimeException e)
			{
				throw new VTLNestedException("Syntax error defining domain " + alias, e);
			}
			
			if (statements.size() != 1)
				throw new InvalidParameterException("Invalid domain definition expression: " + code);
			
			Statement statement = statements.get(0);
			if (!(statement instanceof DMLStatement))
				throw new InvalidParameterException("Invalid domain definition expression: " + code);
			
			VTLValueMetadata meta;
			try
			{
				meta = ((DMLStatement) statement).getMetadata(new TransformationCriterionScope(alias, parentDomain));
			}
			catch (VTLException e)
			{
				throw new VTLNestedException("Error creating domain " + alias + " with expression " + code, e);
			}
			
			if (meta.isDataSet())
				throw new InvalidParameterException("Invalid domain definition expression: " + code);
			
			if (!(((ScalarValueMetadata<?, ?>) meta).getDomain() instanceof BooleanDomainSubset))
				throw new InvalidParameterException("Invalid domain definition expression: " + code);
			
			if (parentDomain instanceof StringDomainSubset)
				domains.put(alias, new StringTransformationDomainSubset(alias, (StringDomain) parentDomain, (Transformation) statement));
			else if (parentDomain instanceof IntegerDomainSubset)
				domains.put(alias, new IntegerTransformationDomainSubset(alias, (IntegerDomain) parentDomain, (Transformation) statement));
			else if (parentDomain instanceof NumberDomainSubset)
				domains.put(alias, new NumberTransformationDomainSubset(alias, (NumberDomain) parentDomain, (Transformation) statement));
			else
				LOGGER.warn("Ignoring unsupported described domain type {}[{}].", alias, parent);
		}
		else
			LOGGER.warn("Ignoring unsupported domain type {}[{}].", alias, parent);
		
		// always return the defined domain, using side effects to directly store the new domain 
		return domains.get(alias);
	}
}
