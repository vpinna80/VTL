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

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getLocalPropertyValue;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.registerSupportedProperties;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.mapValue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleStructuresException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingValueException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class VTLExamplesEnvironment implements Environment, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLExamplesEnvironment.class);
	private static final Pattern TOKEN_PATTERN = Pattern.compile("(?<=,|\r\n|\n|^)(\"(?:\"\"|[^\"])*\"|([^\",\r\n]*))(?=,|\r\n|\n|$)");
	private static final Map<String, List<String>> OPERATORS = new LinkedHashMap<>();
	private static final Set<String> EXCLUDED_OPERATORS = Set.of("Pivoting", "Random", "Persistent assignment", 
		"Duration to number days", "Fill time series", "Number days to duration");
	
	public static final VTLProperty EXAMPLES_CATEGORY = new VTLPropertyImpl("vtl.examples.category", "Operator category", "", EnumSet.of(IS_REQUIRED));
	public static final VTLProperty EXAMPLES_OPERATOR = new VTLPropertyImpl("vtl.examples.operator", "Operator name", "", EnumSet.of(IS_REQUIRED));
	
	static
	{
		registerSupportedProperties(VTLExamplesEnvironment.class, EXAMPLES_CATEGORY, EXAMPLES_OPERATOR);
		
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(VTLExamplesEnvironment.class.getResourceAsStream("exampleslist.txt"), UTF_8)))
		{
			reader.lines().forEach(line -> {
				String[] elems = line.split(",");
				String category = elems[0];
				String operator = elems[1];
				
				if (!EXCLUDED_OPERATORS.contains(operator))
					OPERATORS.merge(category, List.of(operator), (l1, l2) -> {
						List<String> res = new ArrayList<>(l1);
						res.addAll(l2);
						return res;
					});
			});
		}
		catch (Exception e)
		{
			throw new ExceptionInInitializerError(e);
		}
	}
	
	public static List<String> getCategories() throws IOException
	{
		return new ArrayList<>(OPERATORS.keySet());
	}
	
	public static List<String> getOperators(String category) throws IOException
	{
		return OPERATORS.get(category);
	}
	
	public static URL computeJsonURL(String category, String operator)
	{
		return VTLExamplesEnvironment.class.getResource("examples/" + category + "/" + operator + "/examples.json");
	}
	
	public static URL computeCodeURL(String category, String operator)
	{
		return VTLExamplesEnvironment.class.getResource("examples/" + category + "/" + operator + "/examples.vtl");
	}

	private final String[][] inputs;
	
	public VTLExamplesEnvironment() throws IOException, URISyntaxException
	{
		this(getLocalPropertyValue(EXAMPLES_CATEGORY), getLocalPropertyValue(EXAMPLES_OPERATOR));
	}
	
	public VTLExamplesEnvironment(String category, String operator) throws IOException, URISyntaxException
	{
		if (category == null || category.isBlank())
			throw new InvalidParameterException("Example category not specified.");
		if (operator == null || operator.isBlank())
			throw new InvalidParameterException("Example operator not specified.");
		
		LOGGER.info("Initializing example for operator {}", operator);
		List<String[]> csv_lines = new ArrayList<>();
		for(int i = 1; true; i++)
		{
			URL csv = VTLExamplesEnvironment.class.getResource("examples/" + category + "/" + operator + "/ds_" + i + ".csv");
			if (csv == null)
				break;
				
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(csv.openStream(), UTF_8)))
			{
				csv_lines.add(reader.lines().collect(toList()).toArray(String[]::new));
			}
		}
		inputs = csv_lines.toArray(String[][]::new);
		LOGGER.info("Loaded {} datasets for {}", inputs.length, operator);
	}
	
	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias alias)
	{
		if (repo == null)
			return Optional.empty();
		
		for (int i = 0; i < inputs.length; i++)
			if (VTLAliasImpl.of("ds_" + (i + 1)).equals(alias))
			{
				DataSetStructure structure = repo.getMetadata(alias)
					.map(DataSetStructure.class::cast)
					.orElseThrow(() -> new VTLUndefinedObjectException("Metadata", alias));
				
				return Optional.of(streamInput(inputs[i], structure));
			}
		
		return Optional.empty();
	}

	private DataSet streamInput(String[] input, DataSetStructure structure)
	{
		List<DataSetComponent<?, ?, ?>> metadata;
		
		// match each column header to a component 
		String[] fields = input[0].split(",");
		metadata = new ArrayList<>();
		
		for (String field: fields)
			metadata.add(structure.getComponent(VTLAliasImpl.of(field)).orElseThrow(() -> new IllegalStateException("Unknown CSV field " + field + " for structure " + structure)));
		for (DataSetComponent<?, ?, ?> comp: structure)
			if (!metadata.contains(comp))
				throw new VTLMissingComponentsException(metadata, comp);
		
		if (!metadata.containsAll(structure) || !structure.containsAll(metadata))
			throw new VTLIncompatibleStructuresException("Reading csv", structure, metadata);

		return new StreamWrapperDataSet(structure, () -> Arrays.stream(input, 1, input.length)
				.filter(line -> !line.trim().isEmpty())
				.map(line -> lineToDPBuilder(line.replace("\u202C", "").replace("\u202A", ""), metadata))
				.map(b -> b.build(LineageExternal.of("example"), structure))
			, true);
	}

	private DataPointBuilder lineToDPBuilder(String line, List<DataSetComponent<?, ?, ?>> metadata)
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
				DataSetComponent<?, ?, ?> component = metadata.get(count);
				ScalarValue<?, ?, ?, ?> value = mapValue(component.getDomain(), token, null);
				
				if (value.isNull() && component.is(Identifier.class))
					throw new NullPointerException("Parsed a null value for identifier " + component + ": " + token);
					
				builder.add(component, value);
				count++;
			}
			else
				throw new VTLMissingValueException(metadata.get(count), line);
		
		return builder;
	}
}
