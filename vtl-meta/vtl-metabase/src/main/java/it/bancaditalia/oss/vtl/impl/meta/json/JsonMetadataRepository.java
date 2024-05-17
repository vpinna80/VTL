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
import static it.bancaditalia.oss.vtl.util.Utils.splitting;

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
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.json.JsonMapper;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.VariableImpl;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
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

	private final Map<String, Variable<?, ?>> variables = new HashMap<>(); 
	private final Map<String, DataSetMetadata> structures = new HashMap<>(); 
	private final Map<String, String> sources = new HashMap<>(); 
	private final Map<String, String> datasets = new HashMap<>(); 
	
	public JsonMetadataRepository() throws IOException
	{
		String url = JSON_METADATA_URL.getValue();
		if (url == null || url.isEmpty())
			throw new IllegalStateException("No url configured for json metadata repository.");

		Map<String, Entry<String, String>> dsDefs;
		Map<String, Map<String, Class<? extends Component>>> strDefs;
		Map<String, String> varDefs;
		Map<String, Object> domDefs;

		try (InputStream source = new URL(url).openStream())
		{
			Map<?, ?> json = JsonFactory.builder().build().setCodec(new JsonMapper()).createParser(source).readValueAs(Map.class);
			
			dsDefs = iterate((List<?>) json.get("datasets"), this::createDataset);
			strDefs = iterate((List<?>) json.get("structures"), this::createStructure);
			varDefs = iterate((List<?>) json.get("variables"), this::createVariable);
			domDefs = iterate((List<?>) json.get("domains"), this::createDomain);
		}

		varDefs.forEach((n, d) -> variables.put(n, VariableImpl.of(n, getDomain(d))));
		strDefs.forEach((n, l) -> structures.put(n, l.entrySet().stream().map(splitting((c, r) -> {
			Objects.requireNonNull(variables.get(c));
			return variables.get(c).as(r); 
		})).collect(toDataStructure())));
		dsDefs.forEach((n, e) -> {
			Objects.requireNonNull(structures.get(e.getKey()), "Undefined structure: " + e.getKey());
			datasets.put(n, e.getKey());
			sources.put(n, e.getValue());
		});
	}
	
	@Override
	public DataSetMetadata getStructure(String name)
	{
		String structureRef = datasets.get(name);
		return structureRef != null ? structures.get(structureRef) : null;
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
	
	@Override
	public String getDatasetSource(String name)
	{
		String source = sources.get(name);
		
		return source != null ? source : super.getDatasetSource(name);
	}
	
	private <T> Map<String, T> iterate(List<?> list, SerBiFunction<String, Map<?, ?>, T> processor)
	{
		Map<String, T> result = new HashMap<>();

		if (list != null)
			for (Object entry: list)
			{
				if (entry instanceof Map)
				{
					Map<?, ?> obj = (Map<?, ?>) entry;
					if (!obj.containsKey("name"))
						throw new IllegalStateException("missing name for object");
					if (!(obj.get("name") instanceof String))
						throw new IllegalStateException("object name is not a string");
					
					String name = (String) obj.get("name");
					if (result.containsKey(name))
						throw new IllegalStateException("Metadata object " + name + " is defined more than once.");
					result.put(name, processor.apply(name, obj));
				}
				else
					throw new InvalidParameterException("Expected JSON object but found " + entry.getClass());
			}
		
		return result;
	}
	
	private Entry<String, String> createDataset(String name, Map<?, ?> dataset)
	{
		String structureRef = (String) dataset.get("structure");
		String source = (String) dataset.get("source");
		
		LOGGER.debug("Found dataset {} with structure {}", name, structureRef);
		return new SimpleEntry<>(structureRef, source);
	}

	private Map<String, Class<? extends Component>> createStructure(String name, Map<?, ?> jsonStructure)
	{
		LOGGER.info("Found structure {}", name);
		return iterate((List<?>) jsonStructure.get("components"), this::createComponent);
	}

	private Class<? extends Component> createComponent(String name, Map<?, ?> jsonStructure)
	{
		return ROLES.get(jsonStructure.get("role"));
	}

	private String createVariable(String name, Map<?, ?> variable)
	{
		return (String) variable.get("domain");
	}
	
	private Object createDomain(String name, Map<?, ?> domain)
	{
		return null;
//		Object parent = ((Map<?, ?>) domain).get("parent");
//		if (parent == null || !(parent instanceof String))
//			throw new InvalidParameterException("Parent domain invalid or not specified for " + name + ".");
//
//		LOGGER.debug("Found domain {}", name);
//		Object enumerated = domain.get("enumerated");
//		if (enumerated instanceof List)
//			if ("string".equals(parent))
//			{
//				Set<String> codes = ((List<?>) enumerated).stream().map(String.class::cast).collect(toSet());
//				LOGGER.debug("Obtained {} codes for {}", codes.size(), name);
//				defineDomain(name, new StringCodeList(STRINGDS, name, codes));
//			}
//			else
//				LOGGER.warn("Ignoring unsupported domain {}[{}].", name, parent);
//		else if (enumerated != null)
//			throw new InvalidParameterException("Invalid enumerated domain definition for " + name + ".");
//		else
//			LOGGER.warn("Ignoring unsupported domain type {}[{}].", name, parent);
	}
}
