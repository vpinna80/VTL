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

import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.extractMetadata;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.mapValue;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static java.util.Objects.isNull;

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
import java.util.Map.Entry;
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
import it.bancaditalia.oss.vtl.impl.types.dataset.BiFunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class CSVPathEnvironment implements Environment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CSVPathEnvironment.class);
	private static final Pattern TOKEN_PATTERN = Pattern.compile("(?<=,|\r\n|\n|^)(\"(?:\"\"|[^\"])*\"|([^\",\r\n]*))(?=,|\r\n|\n|$)");

	public static final VTLProperty VTL_CSV_ENVIRONMENT_SEARCH_PATH = 
			new VTLPropertyImpl("vtl.csv.search.path", "Path to search for CSV files", System.getenv("VTL_PATH"), EnumSet.of(IS_REQUIRED), System.getenv("VTL_PATH"));

	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(CSVPathEnvironment.class, VTL_CSV_ENVIRONMENT_SEARCH_PATH);
	}
	
	private List<Path> paths;
	
	public CSVPathEnvironment()
	{
		this(VTL_CSV_ENVIRONMENT_SEARCH_PATH.getValues().stream().map(Paths::get).collect(toList()));
	}
	
	public CSVPathEnvironment(List<Path> paths)
	{
		this.paths = paths;
	}
	
	@Override
	public boolean contains(VTLAlias name)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<VTLValueMetadata> getValueMetadata(VTLAlias alias)
	{
		throw new UnsupportedOperationException(alias.toString());
	}
	
	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias alias)
	{
		String source = repo.getDataSource(alias);
		String fileName;
		
		fileName = source.startsWith("csv:") ? source.substring(4) : source == alias.toString() ? source + ".csv" : source;
		LOGGER.debug("Looking for csv file '{}'", fileName);

		return paths.stream()
			.map(path -> path.resolve(fileName))
			.filter(Files::exists)
			.limit(1)
			.peek(path -> LOGGER.info("Found {} in {}", fileName, path))
			.map(path -> {
				Map<DataStructureComponent<?, ?, ?>, String> masks = new HashMap<>();
				
				VTLValueMetadata repoMeta = repo.getMetadata(alias).orElseThrow(() -> new VTLUndefinedObjectException("Metadata", alias));
				
				// Infer structure from enriched CSV header 
				if (isNull(repoMeta))
					try (BufferedReader reader = Files.newBufferedReader(path))
					{
						Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> metadata = extractMetadata(repo, reader.readLine().split(","));
						DataSetMetadata structure = new DataStructureBuilder(metadata.getKey()).build();
						masks.putAll(metadata.getValue());
						return (VTLValue) new BiFunctionDataSet<>(structure, (p, s) -> streamFileName(p, s, repo, masks), path, structure);
					}
					catch (IOException e)
					{
						throw new VTLNestedException("Exception while reading " + fileName, e);
					}
				else if (repoMeta instanceof ScalarValueMetadata)
					try (BufferedReader reader = Files.newBufferedReader(path))
					{
						String line = null;
						for (int i = 0; i < 2; i++)
							line = reader.readLine();
						return mapValue(((ScalarValueMetadata<?, ?>) repoMeta).getDomain(), line, null);
					}
					catch (IOException e)
					{
						throw new VTLNestedException("Exception while reading " + fileName, e);
					}
				else
				{
					DataSetMetadata structure = (DataSetMetadata) repoMeta;
					return (VTLValue) new BiFunctionDataSet<>(structure, (p, s) -> streamFileName(p, s, repo, masks), path, structure);
				}
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
			metadata = new ArrayList<>();
			
			for (String field: fields)
				metadata.add(structure.getComponent(VTLAliasImpl.of(field)).orElseThrow(() -> new IllegalStateException("Unknown CSV field " + field + " for structure " + structure)));
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
					// remove BIDI code points (often pasted from other software like ms office)
					return lineToDPBuilder(line.replace("\u202C", "").replace("\u202A", ""), metadata, masks);
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
					LOGGER.trace("Parsing string value {} for component {} with mask {}", token, component, masks.get(component));
					ScalarValue<?, ?, ?, ?> value = mapValue(component.getVariable().getDomain(), token, masks.get(component));
					
					if (value.isNull() && component.is(Identifier.class))
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
