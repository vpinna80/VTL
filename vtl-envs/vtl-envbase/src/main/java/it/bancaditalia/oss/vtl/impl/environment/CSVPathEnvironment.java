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
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.REQUIRED;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleStructuresException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils;
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
		paths = Arrays.asList(VTL_CSV_ENVIRONMENT_SEARCH_PATH.getValue().split(","));
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
		if (repo.getDatasetSource(alias).startsWith("csv:"))
		{
			String fileName = repo.getDatasetSource(alias).substring(4);
			LOGGER.debug("Looking for csv file '{}'", fileName);
	
			return paths.stream()
					.map(path -> Paths.get(path, fileName))
					.filter(Files::exists)
					.limit(1)
					.peek(path -> LOGGER.info("Found {} in {}", fileName, path))
					.map(path -> {
				try (BufferedReader reader = Files.newBufferedReader(path))
				{
					// can't use streams, must be ordered for the first line processed to be actually the header 
					final String[] headers = reader.readLine().split(",");
					
					DataSetMetadata structure = Objects.requireNonNull(repo.getStructure(alias), "no structure for " + alias);
					if (structure == null)
						structure = new DataStructureBuilder(extractMetadata(repo, headers).getKey()).build();
					
					return (VTLValue) new BiFunctionDataSet<>(structure, (p, s) -> streamFileName(p, s, repo), path, structure);
				}
				catch (IOException e)
				{
					throw new VTLNestedException("Exception while reading " + fileName, e);
				}
			}).findAny();
		}
		else
			return Optional.empty();
	}

	protected Stream<DataPoint> streamFileName(Path path, DataSetMetadata structure, MetadataRepository repo)
	{
		try (BufferedReader reader = Files.newBufferedReader(path))
		{
			List<DataStructureComponent<?, ?, ?>> metadata;
			Map<DataStructureComponent<?, ?, ?>, String> masks;
			String[] fields = reader.readLine().split(",");
			
			if (structure == null)
			{
				Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> headerInfo = extractMetadata(repo, fields);
				metadata = headerInfo.getKey();
				masks = headerInfo.getValue();
			}
			else
			{
				metadata = new ArrayList<>();
				for (String field: fields)
					metadata.add(structure.getComponent(field).orElseThrow(() -> new IllegalStateException("Unknown CSV field " + field + " for structure " + structure)));
				for (DataStructureComponent<?, ?, ?> comp: structure)
					if (!metadata.contains(comp))
						throw new VTLMissingComponentsException(comp, metadata);
				masks = new HashMap<>();
			}
			
			if (!metadata.containsAll(structure) || !structure.containsAll(metadata))
				throw new VTLIncompatibleStructuresException("Reading csv", structure, metadata);
			
			LOGGER.info("Counting lines on {}...", path);
			long lineCount = countLines(reader);
			LOGGER.info("Reading {} lines from {}...", lineCount, path);
	
			// Do not close this reader!
			BufferedReader innerReader = Files.newBufferedReader(path);
			
			// Skip header
			innerReader.readLine();
			
			return ProgressWindow.of("Loading CSV", lineCount, innerReader.lines())
				// Skip empty lines
				.filter(line -> !line.trim().isEmpty())
				.peek(line -> LOGGER.trace("Parsing line from CSV: {}", line))
				.map(line -> {
					DataPointBuilder builder = new DataPointBuilder();

					// Perform split by repeatedly matching the line against the regex
					int count = 0;
					Matcher tokenizer = TOKEN_PATTERN.matcher(line);
					// match only the declared components, skip remaining values
					while (count < metadata.size())
						if (tokenizer.find())
						{
							// 1 is the matched token
							String token = tokenizer.group(1);
							// group 2 is matched if the string field is not quoted
							if (Objects.isNull(tokenizer.group(2)))
								// dequote quoted string and replace ""
								token = token.replaceAll("^\"(.*)\"$", "$1").replaceAll("\"\"", "\"");
							else
								// trim unquoted string
								token = token.trim();

							// parse field value into a VTL scalar 
							DataStructureComponent<?, ?, ?> component = metadata.get(count);
							ScalarValue<?, ?, ?, ?> value = CSVParseUtils.mapValue(component, token, masks.get(component));
							
							if (value instanceof NullValue && component.is(Identifier.class))
								throw new NullPointerException("Parsed a null value for identifier " + component + ": " + token);
								
							builder.add(component, value);
							count++;
						}
						else
							throw new IllegalStateException("While parsing " + path + ": Expected value for " + metadata.get(count) + " but the row ended before it:\n" + line);
					if (tokenizer.end() < line.length() - 1)
						LOGGER.warn("Skipped trailing characters in line: " + line.substring(tokenizer.end() + 1));
					
					return builder;
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
						throw new UncheckedIOException(e);
					}
				});
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + path, e);
		}
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
