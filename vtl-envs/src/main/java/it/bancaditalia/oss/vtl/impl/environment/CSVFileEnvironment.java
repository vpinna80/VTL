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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.util.ProgressWindow;
import it.bancaditalia.oss.vtl.util.Utils;

public class CSVFileEnvironment implements Environment
{
	private static final Pattern TOKEN_PATTERN = Pattern.compile("(?:,|\n|^)(\"(?:(?:\"\")*[^\"]*)*\"|([^\",\n]*)|(?:\n|$))");
	public static final Logger LOGGER = LoggerFactory.getLogger(CSVFileEnvironment.class);
	
	@Override
	public boolean contains(String name)
	{
		return name.startsWith("csv:") && Files.exists(Paths.get(name.substring(4)));
	}

	@Override
	public Optional<VTLValue> getValue(String name)
	{
		if (!contains(name))
			return Optional.empty();

		String fileName = name.substring(4);
		
		LOGGER.debug("Looking for csv file '{}'", fileName);

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), UTF_8)))
		{
			// can't use streams, must be ordered for the first line processed to be actually the header 
			final DataSetMetadata structure = new DataStructureBuilder(extractMetadata(reader.readLine().split(",")).getKey()).build();
			
			return Optional.of(new LightFDataSet<>(structure, this::streamFileName, fileName));
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + fileName, e);
		}
	}
	
	@SuppressWarnings("resource")
	protected Stream<DataPoint> streamFileName(String fileName)
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), UTF_8)))
		{
			Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> headerInfo = extractMetadata(reader.readLine().split(","));
			List<DataStructureComponent<?, ?, ?>> metadata = headerInfo.getKey();
			Map<DataStructureComponent<?, ?, ?>, String> masks = headerInfo.getValue();
			final DataSetMetadata structure = new DataStructureBuilder(metadata).build();

			LOGGER.info("Counting lines on {}...", fileName);
			long lineCount = countLines(reader);
			LOGGER.info("Reading {} lines from {}...", lineCount, fileName);
	
			// Do not close this reader!
			BufferedReader innerReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), UTF_8));
			
			// Skip header
			innerReader.readLine();
			
			Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, Boolean> set = new ConcurrentHashMap<>();
			return ProgressWindow.of("Loading CSV", lineCount, Utils.getStream(innerReader.lines()))
				// Skip empty lines
				.filter(line -> !line.trim().isEmpty())
				.peek(line -> LOGGER.trace("Parsing line from CSV: {}", line))
				.map(line -> {
					Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>();

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
							if (isNull(tokenizer.group(2)))
								// dequote quoted string and replace ""
								token = token.replaceAll("^\"(.*)\"$", "$1").replaceAll("\"\"", "\"");
							else
								// trim unquoted string
								token = token.trim();
							// parse field value into a VTL scalar 
							DataStructureComponent<?, ?, ?> component = metadata.get(count);
							result.put(component, CSVParseUtils.mapValue(component, token, masks.get(component)));
							count++;
						}
						else
							throw new IllegalStateException("Expected value for " + metadata.get(count) + " but the row ended before it:\n" + line);
					if (tokenizer.end() < line.length() - 1)
						LOGGER.warn("Skipped trailing characters in line: " + line.substring(tokenizer.end() + 1));
					
					return result;
				})
				.map(m -> new DataPointBuilder(m).build(LineageExternal.of("csv:" + fileName), structure))
				.peek(dp -> LOGGER.trace("Parsed datapoint from CSV: {}", dp))
				.peek(dp -> {
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> values = dp.getValues(Identifier.class);
					Boolean a = set.putIfAbsent(values, true);
					if (a != null)
						throw new IllegalStateException("Identifiers are not unique: " + values);
				}).onClose(() -> {
					try
					{
						LOGGER.info("Completed reading of {}.", fileName);
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
			throw new VTLNestedException("Exception while reading " + fileName, e);
		}
	}

	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String name)
	{
		if (!contains(name))
			return Optional.empty();

		String fileName = name.substring(4);

		LOGGER.debug("Looking for csv file '{}'", fileName);

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), UTF_8)))
		{
			return Optional.of(new DataStructureBuilder(extractMetadata(reader.readLine().split(",")).getKey()).build());
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + fileName, e);
		}
	}
	
	public static int countLines(Reader is) throws IOException
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
