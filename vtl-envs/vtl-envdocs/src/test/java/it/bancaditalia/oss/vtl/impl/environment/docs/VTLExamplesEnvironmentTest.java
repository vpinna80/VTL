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
package it.bancaditalia.oss.vtl.impl.environment.docs;

import static it.bancaditalia.oss.vtl.impl.environment.docs.VTLExamplesEnvironment.computeCodeURL;
import static it.bancaditalia.oss.vtl.impl.environment.docs.VTLExamplesEnvironment.getCategories;
import static it.bancaditalia.oss.vtl.impl.environment.docs.VTLExamplesEnvironment.getOperators;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingJsonFactory;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class VTLExamplesEnvironmentTest
{
	public static Stream<Arguments> getOperatorsTest() throws IOException
	{
		List<Arguments> instances = new ArrayList<>();
		
		for (String category: getCategories())
			for (String operator: getOperators(category))
				instances.add(Arguments.of(category, operator));
		
		return instances.stream();
	}

	@ParameterizedTest(name = "{0} - {1}")
	@MethodSource
	public void getOperatorsTest(String category, String operator) throws IOException, URISyntaxException
	{
		VTLExamplesEnvironment environment = new VTLExamplesEnvironment(category, operator);
		
		
		URL json = VTLExamplesEnvironment.computeJsonURL(category, operator);
		MetadataRepository repo = new JsonMetadataRepository(null, json, mock(Engine.class));
		
		List<VTLAlias> inputAliases = new ArrayList<>();
		try (InputStream source = json.openStream(); JsonParser parser = new MappingJsonFactory().createParser(source))
		{
			TypeReference<List<Map<String, Object>>> typeRef = new TypeReference<>() {};
			List<Map<String, Object>> datasets = parser.readValueAsTree().get("data").traverse(parser.getCodec()).readValueAs(typeRef);
			inputAliases = datasets.stream().map(map -> VTLAliasImpl.of((String) map.get("name"))).collect(toList());
		}
		
		URL codeURL = computeCodeURL(category, operator);
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(codeURL.openStream(), UTF_8)))
		{
			assertTrue(reader.lines().count() > 19, "Invalid VTL example code");
		}
		
		for (VTLAlias alias: inputAliases)
		{
			Optional<DataSet> optional = environment.getValue(repo, alias).map(DataSet.class::cast);
			assertTrue(optional.isPresent(), "dataset " + alias + " not found.");
			optional.get().forEach(dp -> {});
		}
	}
}
