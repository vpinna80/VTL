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

import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.REQUIRED;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder.toDataStructure;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
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
import it.bancaditalia.oss.vtl.exceptions.VTLDuplicatedObjectException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerBiFunction;

public class JsonMetadataRepository extends InMemoryMetadataRepository
{
	public static final VTLProperty JSON_METADATA_URL = new VTLPropertyImpl("vtl.json.metadata.url", "Json url providing structures and domains", "file://", EnumSet.of(REQUIRED));

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
	private final Map<VTLAlias, DataSetMetadata> structures = new HashMap<>(); 
	private final Map<VTLAlias, String> sources = new HashMap<>(); 
	private final Map<VTLAlias, VTLAlias> datasets = new HashMap<>(); 
	
	public JsonMetadataRepository() throws IOException
	{
		this(JSON_METADATA_URL.getValue());
	}
	
	public JsonMetadataRepository(String jsonURL) throws IOException
	{
		if (jsonURL == null || jsonURL.isEmpty())
			throw new IllegalStateException("No url configured for json metadata repository.");

		Map<VTLAlias, Entry<VTLAlias, String>> dsDefs;
		Map<VTLAlias, Map<VTLAlias, Class<? extends Component>>> strDefs;
		Map<VTLAlias, VTLAlias> varDefs;

		try (InputStream source = new URL(jsonURL).openStream())
		{
			Map<?, ?> json;
			try (JsonParser parser = JsonFactory.builder().build().setCodec(new JsonMapper()).createParser(source))
			{
				json = parser.readValueAs(Map.class);
			}
			
			dsDefs = iterate(json, "dataset", this::createDataset);
			strDefs = iterate(json, "structure", this::createStructure);
			varDefs = iterate(json, "variable", this::createVariable);
			iterate(json, "domain", this::createDomain);
		}

		varDefs.forEach((n, d) -> variables.put(n, VariableImpl.of(n, getDomain(d))));
		strDefs.forEach((n, l) -> structures.put(n, l.entrySet().stream().map(splitting((c, r) -> {
			requireNonNull(variables.get(c), "Undefined variable " + c);
			return variables.get(c).as(r); 
		})).collect(toDataStructure())));
		dsDefs.forEach((n, e) -> {
			VTLAlias strName = e.getKey();
			if (!structures.containsKey(strName))
				throw new VTLUndefinedObjectException("Structure", strName);
			datasets.put(n, strName);
			sources.put(n, e.getValue());
		});
	}
	
	@Override
	public Optional<DataSetMetadata> getStructure(VTLAlias name)
	{
		return Optional.ofNullable(datasets.get(name)).map(structures::get);
	}
	
	@Override
	public Variable<?, ?> getVariable(VTLAlias alias)
	{
		Variable<?, ?> variable = variables.get(alias);
		if (variable != null)
			return variable;
		else
			return super.getVariable(alias);
	}
	
	@Override
	public String getDatasetSource(VTLAlias name)
	{
		String source = sources.get(name);
		
		return source != null ? source : super.getDatasetSource(name);
	}
	
	private <T> Map<VTLAlias, T> iterate(Map<?, ?> json, String element, SerBiFunction<VTLAlias, Map<?, ?>, T> processor)
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
	
	private Entry<VTLAlias, String> createDataset(VTLAlias name, Map<?, ?> dataset)
	{
		VTLAlias structureRef = VTLAliasImpl.of((String) dataset.get("structure"));
		String source = (String) dataset.get("source");
		
		LOGGER.debug("Found dataset {} with structure {}", name, structureRef);
		return new SimpleEntry<>(structureRef, source);
	}

	private Map<VTLAlias, Class<? extends Component>> createStructure(VTLAlias name, Map<?, ?> jsonStructure)
	{
		LOGGER.info("Found structure {}", name);
		return iterate(jsonStructure, "component", this::createComponent);
	}

	private Class<? extends Component> createComponent(VTLAlias name, Map<?, ?> jsonStructure)
	{
		return ROLES.get(jsonStructure.get("role"));
	}

	private VTLAlias createVariable(VTLAlias name, Map<?, ?> variable)
	{
		return VTLAliasImpl.of((String) variable.get("domain"));
	}
	
	private ValueDomainSubset<?, ?> createDomain(VTLAlias name, Map<?, ?> domainDef)
	{
		Object parent = ((Map<?, ?>) domainDef).get("parent");
		if (parent == null || !(parent instanceof String))
			throw new InvalidParameterException("Parent domain invalid or not specified for " + name + ".");

		LOGGER.debug("Found domain {}", name);
		Object enumerated = domainDef.get("enumerated");
		if (enumerated instanceof List)
			if ("string".equals(parent))
			{
				Set<String> codes = ((List<?>) enumerated).stream().map(String.class::cast).collect(toSet());
				LOGGER.debug("Obtained {} codes for {}", codes.size(), name);
				StringCodeList domain = new StringCodeList(STRINGDS, name, codes);
				defineDomain(name, domain);
				return domain;
			}
			else
				LOGGER.warn("Ignoring unsupported domain {}[{}].", name, parent);
		else if (enumerated != null)
			throw new InvalidParameterException("Invalid enumerated domain definition for " + name + ".");
		else
			LOGGER.warn("Ignoring unsupported domain type {}[{}].", name, parent);
		
		return null;
	}
}
