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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.json.JsonMapper;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class JsonMetadataRepository extends InMemoryMetadataRepository
{
	public static final VTLProperty JSON_METADATA_URL = new VTLPropertyImpl("vtl.json.metadata.url", "Json url providing structures and domains", "file://", EnumSet.of(REQUIRED));

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonMetadataRepository.class);
	private static final Map<String, Class<? extends ComponentRole>> ROLE_ELEMENTS = new HashMap<>(); 
	
	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(JsonMetadataRepository.class, JSON_METADATA_URL);
		
		ROLE_ELEMENTS.put("identifiers", Identifier.class);
		ROLE_ELEMENTS.put("measures", Measure.class);
		ROLE_ELEMENTS.put("attributes", Attribute.class);
		ROLE_ELEMENTS.put("viralAttributes", ViralAttribute.class);
	}

	private final Map<String, ValueDomainSubset<?, ?>> variables = new HashMap<>(); 
	private final Map<String, DataSetMetadata> structures = new HashMap<>(); 
	private final Map<String, String> datasets = new HashMap<>(); 
	
	public JsonMetadataRepository() throws IOException
	{
		String url = JSON_METADATA_URL.getValue();
		if (url == null || url.isEmpty())
			throw new IllegalStateException("No url configured for json metadata repository.");

		try (InputStream source = new URL(url).openStream())
		{
			@SuppressWarnings("unchecked")
			Map<?, Map<?, ?>> json = JsonFactory.builder().build().setCodec(new JsonMapper()).createParser(source).readValueAs(Map.class);
			
			readDomains(json.get("domains"));
			readVariables((List<Map<String, ?>>) json.get("variables"));
			readStructures((List<Map<String, ?>>) json.get("structures"));
			readDatasets((List<Map<String, ?>>) json.get("datasets"));
		}
	}
	
	@Override
	public DataSetMetadata getStructure(String name)
	{
		String structureFor = datasets.get(name);
		return structureFor != null ? structures.get(structureFor) : null;
	}
	
	private void readDomains(Map<?, ?> domainsSource)
	{
		for (Entry<?, ?> domainEntry: domainsSource.entrySet())
		{
			Object name = domainEntry.getKey();
			Object domainDef = domainEntry.getValue();
			if (name == null || !(name instanceof String))
				throw new IllegalStateException("Found domain missing name.");
			if (domainDef == null || !(domainDef instanceof Map))
				throw new IllegalStateException("Found domain with missing or invalid definition.");

			Object parent = ((Map<?, ?>) domainDef).get("parent");
			if (parent == null || !(parent instanceof String))
				throw new UnsupportedOperationException("Parent domain invalid or not specified for " + name + ".");

			LOGGER.debug("Found domain {}", name);
			if (((Map<?, ?>) domainDef).containsKey("codes") && "string".equals(parent))
			{
				// Fail-fast casting
				Set<String> codes = ((List<?>) ((Map<?, ?>) domainDef).get("codes")).stream().map(String.class::cast).collect(toSet());
				LOGGER.debug("Obtained {} codes for {}", codes.size(), name);
				defineDomain((String) name, new StringCodeList(STRINGDS, (String) name, codes));
			}
			else if (((Map<?, ?>) domainDef).containsKey("codes"))
				LOGGER.warn("Ignoring non-string codelist {}[{}].", name, parent);
			else
				LOGGER.warn("Ignoring unsupported domain {}[{}].", name, parent);
		}
	}
	
	private void readVariables(List<Map<String, ?>> variablesSource)
	{
		for (Map<String, ?> variable: variablesSource)
		{
			String name = (String) variable.get("name");
			String domain = (String) variable.get("domain");
			if (name == null || !(name instanceof String))
				throw new IllegalStateException("Found variable without or with invalid name.");
			if (domain == null || !(domain instanceof String))
				throw new UnsupportedOperationException("Found variable without or with invalid domain for " + name + ".");
			LOGGER.debug("Found variable {} with domain {}", name, domain);
			variables.put((String) name, getDomain((String) domain));
		}
	}
	
	private void readStructures(List<Map<String, ?>> list)
	{
		for (Map<String, ?> structureDescriptor: list)
		{
			Object name = structureDescriptor.get("name");
			if (name == null || !(name instanceof String))
				throw new IllegalStateException("Found structure without or with invalid name.");

			DataStructureBuilder builder = new DataStructureBuilder();
			
			for (String role: List.of("identifiers", "measures", "attributes"))
				for (Object varName: (List<?>) structureDescriptor.get(role))
					if (!(varName instanceof String))
						throw new IllegalStateException("Found dataset without or with invalid name.");
					else
						builder.addComponent(DataStructureComponentImpl.of((String) varName, ROLE_ELEMENTS.get(role), variables.get(varName)));

			DataSetMetadata structure = builder.build();
			LOGGER.info("Found structure {}: {}", name, structure);
			structures.put((String) name, structure);
		}
	}

	private void readDatasets(List<Map<String, ?>> list)
	{
		for (Map<String, ?> dataset: list)
		{
			Object name = (String) dataset.get("name");
			Object structure = (String) dataset.get("structure");
			
			if (name == null || !(name instanceof String))
				throw new IllegalStateException("Found dataset without or with invalid name.");
			if (structure == null || !(structure instanceof String) || !structures.containsKey(structure))
				throw new UnsupportedOperationException("Found dataset without or with invalid structure for " + name + ".");
			
			LOGGER.debug("Found dataset {} with structure {}", name, structure);
			datasets.put((String) name, (String) structure);
		}
	}
}
