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
package it.bancaditalia.oss.vtl.util;

import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.mapValue;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleStructuresException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingValueException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine;
import it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository;
import it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;
import jakarta.xml.bind.JAXBException;

public class VTLExamplesEnvironment implements Environment, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Pattern TOKEN_PATTERN = Pattern.compile("(?<=,|\r\n|\n|^)(\"(?:\"\"|[^\"])*\"|([^\",\r\n]*))(?=,|\r\n|\n|$)");
	private static final Map<Entry<String, String>, VTLSession> SESSIONS = new HashMap<>();
	
	private final int nInputs;
	private final String[][] inputs;
	private final String code;
	private final URL jsonURL;
	
	public static List<String> getCategories() throws IOException
	{
		try (BufferedReader reader = getReader("."))
		{
			return reader.lines().collect(toList());
		}
	}
	
	public static List<String> getOperators(String category) throws IOException
	{
		try (BufferedReader reader = getReader(category))
		{
			return reader.lines().collect(toList());
		}
	}
	
	public static synchronized VTLSession createSession(String category, String operator)
	{
		return SESSIONS.computeIfAbsent(new SimpleEntry<>(category, operator), k -> createExample(category, operator));
	}

	private static VTLSession createExample(String category, String operator)
	{
		try
		{
			VTLExamplesEnvironment env = new VTLExamplesEnvironment(category, operator);

			JsonMetadataRepository repo = new JsonMetadataRepository(env.jsonURL, new JavaVTLEngine());
			return new VTLSessionImpl(env.code, repo, new JavaVTLEngine(), List.of(env), new Workspace() {
				private static final long serialVersionUID = 1L;
				
				private final Map<VTLAlias, Statement> rules = new ConcurrentHashMap<>(); 
				
				@Override
				public Workspace addRule(Statement statement)
				{
					rules.put(statement.getAlias(), statement);
					return this;
				}
				
				@Override
				public Optional<Statement> getRule(VTLAlias name)
				{
					return Optional.ofNullable(rules.get(name));
				}
				
				@Override
				public List<Statement> getRules()
				{
					return new ArrayList<>(rules.values());
				}
			});
		}
		catch (IOException | ClassNotFoundException | JAXBException e)
		{
			throw new RuntimeException(e);
		}
	}

	private VTLExamplesEnvironment(String category, String operator) throws IOException
	{
		try (BufferedReader reader = getReader(category + "/" + operator))
		{
			List<String> files = reader.lines().collect(toList());
			nInputs = files.size() - 2;
		}
		
		inputs = new String[nInputs][];
		for (int i = 0; i < nInputs; i++)
		{
			try (BufferedReader reader = getReader(category + "/" + operator + "/ds_" + (i + 1) + ".csv")) 
			{
				inputs[i] = reader.lines().collect(toList()).toArray(String[]::new);
			}
		}
		
		try (BufferedReader reader = getReader(category + "/" + operator + "/examples.vtl"))
		{
			code = reader.lines().collect(joining(lineSeparator()));
		}
		
		jsonURL = VTLExamplesEnvironment.class.getResource("examples/" + category + "/" + operator + "/examples.json");
	}
	
	@Override
	public boolean contains(VTLAlias alias)
	{
		for (int i = 0; i < nInputs; i++)
			if (VTLAliasImpl.of("ds_" + (i + 1)).equals(alias))
				return true;
		
		return false;
	}
	
	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias alias)
	{
		for (int i = 0; i < nInputs; i++)
			if (VTLAliasImpl.of("ds_" + (i + 1)).equals(alias))
			{
				DataSetMetadata structure = repo.getMetadata(alias).map(DataSetMetadata.class::cast).orElseThrow(() -> new VTLUndefinedObjectException("Metadata", alias));
				
				return Optional.of(streamInput(inputs[i], structure));
			}
		
		return Optional.empty();
	}

	private DataSet streamInput(String[] input, DataSetMetadata structure)
	{
		List<DataStructureComponent<?, ?, ?>> metadata;
		
		// match each column header to a component 
		String[] fields = input[0].split(",");
		metadata = new ArrayList<>();
		
		for (String field: fields)
			metadata.add(structure.getComponent(VTLAliasImpl.of(field)).orElseThrow(() -> new IllegalStateException("Unknown CSV field " + field + " for structure " + structure)));
		for (DataStructureComponent<?, ?, ?> comp: structure)
			if (!metadata.contains(comp))
				throw new VTLMissingComponentsException(comp, metadata);
		
		if (!metadata.containsAll(structure) || !structure.containsAll(metadata))
			throw new VTLIncompatibleStructuresException("Reading csv", structure, metadata);

		return new StreamWrapperDataSet(structure, () -> Arrays.stream(input, 1, input.length)
				.filter(line -> !line.trim().isEmpty())
				.map(line -> lineToDPBuilder(line.replace("\u202C", "").replace("\u202A", ""), metadata))
				.map(b -> b.build(LineageExternal.of("example"), structure))
			, true);
	}

	private DataPointBuilder lineToDPBuilder(String line, List<DataStructureComponent<?, ?, ?>> metadata)
	{
		DataPointBuilder builder = new DataPointBuilder();

		// Perform split by repeatedly matching the line against the regex
		int count = 0;
		Matcher tokenizer = TOKEN_PATTERN.matcher(line);
		// match only the declared components, skip remaining values
		while (count < metadata.size())
			if (tokenizer.find())
			{
				// group 1 is the matched token
				String token = tokenizer.group(1);
				// group 2 is matched if the string field is not quoted
				if (isNull(tokenizer.group(2)))
					// dequote quoted string and replace ""
					token = token.replaceAll("^\"(.*)\"$", "$1").replaceAll("\"\"", "\"");
				else
					// trim unquoted string
					token = token.trim();

				// parse field value into a VTL scalar 
				DataStructureComponent<?, ?, ?> component = metadata.get(count);
				ScalarValue<?, ?, ?, ?> value = mapValue(component.getVariable().getDomain(), token, null);
				
				if (value.isNull() && component.is(Identifier.class))
					throw new NullPointerException("Parsed a null value for identifier " + component + ": " + token);
					
				builder.add(component, value);
				count++;
			}
			else
				throw new VTLMissingValueException(metadata.get(count), line);
		
		return builder;
	}

	private static BufferedReader getReader(String path)
	{
		String resName = "./examples/" + path;
		InputStream res = VTLExamplesEnvironment.class.getResourceAsStream(resName);
		
		requireNonNull(res, "Could not load resource " + resName);
		return new BufferedReader(new InputStreamReader(res, UTF_8));
	}
}
