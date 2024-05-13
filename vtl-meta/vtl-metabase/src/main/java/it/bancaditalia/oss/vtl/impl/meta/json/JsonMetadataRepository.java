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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.json.JsonMapper;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerBiConsumer;

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

	private final Map<String, Variable<?, ?>> variables = new HashMap<>(); 
	private final Map<String, DataSetMetadata> structures = new HashMap<>(); 
	private final Map<String, String> datasets = new HashMap<>(); 
	
	public JsonMetadataRepository() throws IOException
	{
		String url = JSON_METADATA_URL.getValue();
		if (url == null || url.isEmpty())
			throw new IllegalStateException("No url configured for json metadata repository.");

		try (InputStream source = new URL(url).openStream())
		{
			Map<?, ?> json = JsonFactory.builder().build().setCodec(new JsonMapper()).createParser(source).readValueAs(Map.class);
			
			iterate((List<?>) json.get("domains"), this::createDomain);
			iterate((List<?>) json.get("variables"), this::createVariable);
			iterate((List<?>) json.get("structures"), this::createStructure);
			iterate((List<?>) json.get("datasets"), this::createDataset);
		}
	}
	
	@Override
	public DataSetMetadata getStructure(String name)
	{
		String structureFor = datasets.get(name);
		return structureFor != null ? structures.get(structureFor) : null;
	}
	
	@Override
	public Variable<?, ?> getVariable(String alias)
	{
		Variable<?, ?> variable = variables.get(alias);
		if (variable != null)
			return variable;
		else
			return super.getVariable(alias);
	}
	
	private void iterate(List<?> list, SerBiConsumer<String, Map<?, ?>> processor)
	{
		for (Object entry: list)
		{
			if (entry instanceof Map)
			{
				Map<?, ?> obj = (Map<?, ?>) entry;
				if (!obj.containsKey("name"))
					throw new IllegalStateException("missing name for object");
				if (!(obj.get("name") instanceof String))
					throw new IllegalStateException("object name is not a string");
				
				processor.accept((String) obj.get("name"), obj);
			}
			else
				throw new InvalidParameterException("Expected JSON object but found " + entry.getClass());
		}
	}
	
	private void createDomain(String name, Map<?, ?> domain)
	{
		Object parent = ((Map<?, ?>) domain).get("parent");
		if (parent == null || !(parent instanceof String))
			throw new InvalidParameterException("Parent domain invalid or not specified for " + name + ".");

		LOGGER.debug("Found domain {}", name);
		Object enumerated = domain.get("enumerated");
		if (enumerated instanceof List)
			if ("string".equals(parent))
			{
				Set<String> codes = ((List<?>) enumerated).stream().map(String.class::cast).collect(toSet());
				LOGGER.debug("Obtained {} codes for {}", codes.size(), name);
				defineDomain(name, new StringCodeList(STRINGDS, name, codes));
			}
			else
				LOGGER.warn("Ignoring unsupported domain {}[{}].", name, parent);
		else if (enumerated != null)
			throw new InvalidParameterException("Invalid enumerated domain definition for " + name + ".");
		else
			LOGGER.warn("Ignoring unsupported domain type {}[{}].", name, parent);
	}
	
	private void createVariable(String name, Map<?, ?> variable)
	{
		Object domainName = variable.get("domain");
		if (domainName == null || !(domainName instanceof String))
			throw new InvalidParameterException("Invalid domain for variable " + name);
		
		ValueDomainSubset<?, ?> domain = maybeGetDomain((String) domainName)
				.orElseThrow(() -> new InvalidParameterException("Domain " + domainName + " not found for variable " + name));
		
		LOGGER.debug("Found variable {} with domain {}", name, domain);
		variables.put(name, VariableImpl.of(name, domain));
	}
	
	private void createStructure(String name, Map<?, ?> structure)
	{
		DataStructureBuilder builder = new DataStructureBuilder();
		
		Object components = structure.get("components");
		if (components == null || !(components instanceof List))
			throw new InvalidParameterException("components list not found for structure " + name + ".");
		
		for (Object component: (List<?>) components)
			if (component != null && component instanceof Map)
			{
				Object varName = ((Map<?,?>) component).get("name");
				if (!(varName instanceof String))
					throw new InvalidParameterException("Invalid component name in structure " + name);
				if (!variables.containsKey(varName))
					throw new InvalidParameterException("Undefined variable for component " + varName + " in structure " + name);
				
				Object role = ((Map<?,?>) component).get("role");
				if (!(role instanceof String))
					throw new InvalidParameterException("Invalid role for component " + varName + " in structure " + name);
				if (!ROLES.containsKey(role))
					throw new InvalidParameterException("Invalid role " + role + " for component " + varName + " in structure " + name);
				
				builder.addComponent(variables.get(varName).as(ROLES.get(role)));
			}
			else
				throw new InvalidParameterException("Invalid or null component in structure " + name);

		DataSetMetadata meta = builder.build();
		LOGGER.info("Found structure {}: {}", name, meta);
		structures.put(name, meta);
	}

	private void createDataset(String name, Map<?, ?> dataset)
	{
		Object structure = dataset.get("structure");
		
		if (structure == null || !(structure instanceof String) || !structures.containsKey(structure))
			throw new InvalidParameterException("Found dataset without or with invalid structure for " + name + ".");
		
		LOGGER.debug("Found dataset {} with structure {}", name, structure);
		datasets.put(name, (String) structure);
	}
}
