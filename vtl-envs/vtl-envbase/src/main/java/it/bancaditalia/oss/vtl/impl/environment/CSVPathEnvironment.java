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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.extractMetadata;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.mapValue;
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.REQUIRED;
import static it.bancaditalia.oss.vtl.model.data.Variable.normalizeAlias;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleStructuresException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingValueException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.environment.util.ProgressWindow;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.BiFunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class CSVPathEnvironment implements Environment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CSVPathEnvironment.class);
	private static final Pattern TOKEN_PATTERN = Pattern.compile("(?<=,|\r\n|\n|^)(\"(?:\"\"|[^\"])*\"|([^\",\r\n]*))(?=,|\r\n|\n|$)");

	public static final VTLProperty VTL_CSV_ENVIRONMENT_SEARCH_PATH = 
			new VTLPropertyImpl("vtl.csv.search.path", "Path to search for CSV files", System.getenv("VTL_PATH"), EnumSet.of(REQUIRED), System.getenv("VTL_PATH"));

	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(CSVPathEnvironment.class, VTL_CSV_ENVIRONMENT_SEARCH_PATH);
	}
	
	private List<String> paths;
	
	public CSVPathEnvironment()
	{
		this(VTL_CSV_ENVIRONMENT_SEARCH_PATH.getValues());
	}
	
	public CSVPathEnvironment(List<String> paths)
	{
		this.paths = paths;
	}
	
	@Override
	public boolean contains(String name)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String alias)
	{
		throw new UnsupportedOperationException(alias);
	}
	
	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, String alias)
	{
		String source = repo.getDatasetSource(alias);
		String fileName;
		
		fileName = source.startsWith("csv:") ? source.substring(4) : source == alias ? source + ".csv" : source;
		LOGGER.debug("Looking for csv file '{}'", fileName);

		return paths.stream()
				.map(path -> Paths.get(path, fileName))
				.filter(Files::exists)
				.limit(1)
				.peek(path -> LOGGER.info("Found {} in {}", fileName, path))
				.map(path -> {
					Map<DataStructureComponent<?, ?, ?>, String> masks = new HashMap<>();
					
					DataSetMetadata structure = repo.getStructure(alias)
							.orElseThrow(() -> 
								new VTLUndefinedObjectException("Structure", alias)
							);
					if (isNull(structure))
						try (BufferedReader reader = Files.newBufferedReader(path))
						{
							var metadata = extractMetadata(repo, reader.readLine().split(","));
							structure = new DataStructureBuilder(metadata.getKey()).build();
							masks.putAll(metadata.getValue());
						}
						catch (IOException e)
						{
							throw new VTLNestedException("Exception while reading " + fileName, e);
						}
					
					requireNonNull(structure, "no structure for " + alias);
						
					return (VTLValue) new BiFunctionDataSet<>(structure, (p, s) -> streamFileName(p, s, repo, masks), path, structure);
				}).findAny();
	}

	protected Stream<DataPoint> streamFileName(Path path, DataSetMetadata structure, MetadataRepository repo, Map<DataStructureComponent<?, ?, ?>, String> masks)
	{
		List<DataStructureComponent<?, ?, ?>> metadata;
		long lineCount;
		
		try (BufferedReader reader = Files.newBufferedReader(path))
		{
			// match each column header to a component 
			String[] fields = reader.readLine().split(",");
			for (int i = 0; i < fields.length; i++)
				fields[i] = normalizeAlias(fields[i]);
			
			metadata = new ArrayList<>();
			for (String field: fields)
				metadata.add(structure.getComponent(field).orElseThrow(() -> new IllegalStateException("Unknown CSV field " + field + " for structure " + structure)));
			for (DataStructureComponent<?, ?, ?> comp: structure)
				if (!metadata.contains(comp))
					throw new VTLMissingComponentsException(comp, metadata);
			
			if (!metadata.containsAll(structure) || !structure.containsAll(metadata))
				throw new VTLIncompatibleStructuresException("Reading csv", structure, metadata);
			
			LOGGER.debug("Counting lines on {}...", path);
			lineCount = countLines(reader);
			LOGGER.debug("Reading {} lines from {}...", lineCount, path);
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + path, e);
		}

		BufferedReader innerReader;
		try 
		{
			// Do not close this reader!
			innerReader = Files.newBufferedReader(path);
			// Skip header
			innerReader.readLine();
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + path, e);
		}
		
		return ProgressWindow.of("Loading CSV", lineCount, innerReader.lines())
			// Skip empty lines
			.filter(line -> !line.trim().isEmpty())
			.peek(line -> LOGGER.trace("Parsing line from CSV: {}", line))
			.map(line -> {
				try
				{
					return lineToDPBuilder(line, metadata, masks);
				}
				catch (VTLException e)
				{
					throw new VTLNestedException("Error while reading " + path.getFileName(), e);
				}
			})
			.map(b -> b.build(LineageExternal.of("csv:" + path), structure))
			.peek(dp -> LOGGER.trace("Parsed datapoint from CSV: {}", dp))
				.onClose(() -> {
				try
				{
					LOGGER.info("Completed reading of {}.", path);
					innerReader.close();
				}
				catch (IOException e)
				{
					throw new VTLNestedException("Error while reading " + path.getFileName(), e);
				}
			});
	}

	private DataPointBuilder lineToDPBuilder(String line, List<DataStructureComponent<?, ?, ?>> metadata, Map<DataStructureComponent<?, ?, ?>, String> masks)
	{
		DataPointBuilder builder = new DataPointBuilder();

		// Perform split by repeatedly matching the line against the regex
		int count = 0;
		Matcher tokenizer = TOKEN_PATTERN.matcher(line);
		// match only the declared components, skip remaining values
		while (count < metadata.size())
			if (tokenizer.find())
				try
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
					ScalarValue<?, ?, ?, ?> value = mapValue(component, token, masks.get(component));
					
					if (value instanceof NullValue && component.is(Identifier.class))
						throw new NullPointerException("Parsed a null value for identifier " + component + ": " + token);
						
					builder.add(component, value);
					count++;
				}
				catch (RuntimeException e)
				{
					throw new VTLNestedException("Error reading line " + line, e);
				}
			else
				throw new VTLMissingValueException(metadata.get(count), line);
		if (tokenizer.end() < line.length() - 1)
			LOGGER.warn("Skipped trailing characters in line: " + line.substring(tokenizer.end() + 1));
		
		return builder;
	}

	private static int countLines(Reader is) throws IOException
	{
		char[] c = new char[1024];

        int readChars = is.read(c);
        if (readChars == -1) {
            // bail out if nothing to read
            return 0;
        }

        // make it easy for the optimizer to tune this loop
        int count = 0;
        while (readChars == 1024)
        {
            for (int i=0; i<1024;)
                if (c[i++] == '\n')
                	++count;
            readChars = is.read(c);
        }

        // count remaining characters
        while (readChars != -1)
        {
            for (int i=0; i<readChars; ++i)
                if (c[i] == '\n')
                    ++count;
            readChars = is.read(c);
        }

        return count == 0 ? 1 : count;
	}
}
