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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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
import it.bancaditalia.oss.vtl.impl.meta.subsets.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class JsonMetadataRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonMetadataRepository.class);

	public static final VTLProperty METADATA_JSON_URL = new VTLPropertyImpl("vtl.metadata.json.url", "Json url providing structures and domains", "file://", EnumSet.of(REQUIRED));

	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(JsonMetadataRepository.class, METADATA_JSON_URL);
	}

	private final Map<String, DataSetMetadata> structures = new HashMap<>(); 
	
	public JsonMetadataRepository() throws IOException
	{
		String url = METADATA_JSON_URL.getValue();
		if (url == null || url.isEmpty())
			throw new IllegalStateException("No url configured for json metadata repository.");

		try (InputStream source = new URL(url).openStream())
		{
			@SuppressWarnings("unchecked")
			Map<String, ? extends List<? extends Map<String, ? extends Object>>> json = JsonFactory.builder().build().setCodec(new JsonMapper()).createParser(source).readValueAs(Map.class);
			
			if (json.containsKey("domains"))
				for (Map<String, ? extends Object> domain: json.get("domains"))
				{
					String name = (String) domain.get("name");
					LOGGER.info("Found domain {}", name);
					if (domain.containsKey("codes") && "string".equals(domain.get("parent")))
					{
						@SuppressWarnings("unchecked")
						Set<String> codes = new HashSet<>((List<String>) domain.get("codes"));
						LOGGER.debug("Obtained {} codes for {}", codes.size(), name);
						defineDomain(name, new StringCodeList(STRINGDS, name, codes));
					}
					else
						throw new UnsupportedOperationException(domain.toString());
				}
			if (json.containsKey("datasets"))
				for (Map<String, ?> dataset: json.get("datasets"))
				{
					String name = (String) dataset.get("name");
					DataStructureBuilder builder = new DataStructureBuilder();
					@SuppressWarnings("unchecked")
					List<Map<String, String>> strdesc = (List<Map<String, String>>) dataset.get("structure");
					for (Map<String, String> compdesc: strdesc)
					{
						ValueDomainSubset<?, ?> domain = getDomain(compdesc.get("domain"));
						Class<? extends ComponentRole> role;
						switch (compdesc.get("role"))
						{
							case "identifier": role = Identifier.class; break;
							case "measure": role = Measure.class; break;
							case "attribute": role = Attribute.class; break;
							default: throw new UnsupportedOperationException(compdesc.toString());
						}
						builder.addComponent(DataStructureComponentImpl.of(compdesc.get("name"), role, domain));
					}
					final DataSetMetadata structure = builder.build();
					structures.put(name, structure);
					LOGGER.info("Found structure {}: {}", name, structure);
				}
		}
	}
	
	@Override
	public DataSetMetadata getStructure(String name)
	{
		return structures.get(name);
	}
}
