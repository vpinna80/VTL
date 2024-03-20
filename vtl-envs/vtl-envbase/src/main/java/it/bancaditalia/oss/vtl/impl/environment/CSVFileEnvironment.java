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
import static it.bancaditalia.oss.vtl.impl.environment.util.ProgressWindow.CSV_PROGRESS_BAR_THRESHOLD;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.joining;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils;
import it.bancaditalia.oss.vtl.impl.environment.util.ProgressWindow;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.BiFunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.Utils;

public class CSVFileEnvironment implements Environment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CSVFileEnvironment.class);
	private static final Pattern TOKEN_PATTERN = Pattern.compile("(?<=,|\r\n|\n|^)(\"(?:\"\"|[^\"])*\"|([^\",\r\n]*))(?=,|\r\n|\n|$)");
	
	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(CSVFileEnvironment.class, CSV_PROGRESS_BAR_THRESHOLD);
	}
	
	@Override
	public boolean contains(String name)
	{
		if (!name.startsWith("csv:"))
			return false;
			
		String loc = name.substring(4).split("\\*{4}",2)[0];
		
		try
		{
			if (Files.exists(Paths.get(loc)))
				return true;
		}
		catch (InvalidPathException e)
		{

		}

		try
		{
			new URL(loc).openStream().close();
			return true;
		}
		catch (IOException e)
		{
			return false;
		}
	}

	@Override
	public Optional<VTLValue> getValue(String originalName)
	{
		String name = originalName.contains("****") ? originalName.split("\\*\\*\\*\\*", 2)[0] : originalName;
		if(originalName.contains("****"))
			originalName = originalName.split("\\*\\*\\*\\*", 2)[1];

		if (!contains(name))
			return Optional.empty();

		String fileName = name.substring(4);
		LOGGER.debug("Looking for csv file '{}'", fileName);

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(aliasToInputStream(fileName), UTF_8)))
		{
			// can't use streams, must be ordered for the first line processed to be actually the header 
			final String[] headers = reader.readLine().split(",");
			DataSetMetadata structure = ConfigurationManager.getDefault().getMetadataRepository().getStructure(originalName);
			
			if (structure == null)
				structure = new DataStructureBuilder(extractMetadata(headers).getKey()).build();
			else
				LOGGER.info("CSV structure found in repository.");
			
			return Optional.of(new BiFunctionDataSet<>(structure, this::streamFileName, fileName, originalName));
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + fileName, e);
		}
	}

	private InputStream aliasToInputStream(String fileName) throws MalformedURLException, IOException
	{
		try
		{
			if (Files.exists(Paths.get(fileName)))
				return new FileInputStream(fileName);
		}
		catch (InvalidPathException e)
		{

		}

		return new URL(fileName).openStream();
	}
	
	protected Stream<DataPoint> streamFileName(String fileName, String alias)
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(aliasToInputStream(fileName), UTF_8)))
		{
			DataSetMetadata maybeStructure = ConfigurationManager.getDefault().getMetadataRepository().getStructure(alias);
			List<DataStructureComponent<?, ?, ?>> metadata;
			Map<DataStructureComponent<?, ?, ?>, String> masks;
			
			if (maybeStructure == null)
			{
				Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> headerInfo = extractMetadata(reader.readLine().split(","));
				metadata = headerInfo.getKey();
				masks = headerInfo.getValue();
				LOGGER.debug("Got structure from CSV file: {}", metadata);
				maybeStructure = new DataStructureBuilder(metadata).build();
			}
			else
			{
				DataSetMetadata structure = maybeStructure;
				
				metadata = Arrays.stream(reader.readLine().split(","))
					.map(toEntryWithValue(maybeStructure::getComponent))
					.map(e -> e.getValue().orElseThrow(() -> new VTLMissingComponentsException(e.getKey(), structure)))
					.collect(toList());
				
				masks = metadata.stream()
						.map(toEntryWithValue(c -> c.getVariable().getDomain()))
						.map(keepingKey(ValueDomainSubset::getName))
						.map(keepingKey(CSVParseUtils::mapVarType))
						.map(keepingKey(Entry::getValue))
						.collect(entriesToMap());
			}
			
			DataSetMetadata structure = maybeStructure;

			LOGGER.info("Counting lines on {}...", fileName);
			long lineCount = countLines(reader);
			LOGGER.info("Reading {} lines from {}...", lineCount, fileName);
	
			// Do not close this reader!
			BufferedReader innerReader = new BufferedReader(new InputStreamReader(aliasToInputStream(fileName), UTF_8));
			
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
							if (isNull(tokenizer.group(2)))
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
							throw new IllegalStateException("While parsing " + fileName + ": Expected value for " + metadata.get(count) + " but the row ended before it:\n" + line);
					if (tokenizer.end() < line.length() - 1)
						LOGGER.warn("Skipped trailing characters in line: " + line.substring(tokenizer.end() + 1));
					
					return builder;
				})
				.map(b -> b.build(LineageExternal.of("csv:" + fileName), structure))
				.peek(dp -> LOGGER.trace("Parsed datapoint from CSV: {}", dp))
					.onClose(() -> {
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

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(aliasToInputStream(fileName), UTF_8)))
		{
			final String[] headers = reader.readLine().split(",");
			DataSetMetadata structure = ConfigurationManager.getDefault().getMetadataRepository().getStructure(name);
			
			if (structure == null)
				structure = new DataStructureBuilder(extractMetadata(headers).getKey()).build();
			else
				LOGGER.info("CSV structure found in repository.");

			return Optional.of(structure);
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + fileName, e);
		}
	}
	
	@Override
	public boolean store(VTLValue value, String alias)
	{
		if (!(value instanceof DataSet) || !alias.matches("^'csv:.+'$"))
			return false;
		
		String fileName = alias.substring(5, alias.length() - 1);
		boolean simpleHeader = !fileName.startsWith("*");
		if (!simpleHeader)
			fileName = fileName.substring(1);
		
		try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(fileName))))
		{
			final DataSet ds = (DataSet) value;
			ArrayList<DataStructureComponent<?, ?, ?>> metadata = new ArrayList<>(ds.getMetadata());
			LOGGER.info("Writing csv file in " + fileName);

			String headerLine = metadata.stream()
					.sorted((c1, c2) -> {
						if (c1.is(Attribute.class) && !c2.is(Attribute.class))
							return 1;
						else if (c1.is(Identifier.class) && !c2.is(Identifier.class))
							return -1;
						else if (c1.is(Measure.class) && c2.is(Identifier.class))
							return 1;
						else if (c1.is(Measure.class) && c2.is(Attribute.class))
							return -1;
	
						String n1 = c1.getVariable().getName(), n2 = c2.getVariable().getName();
						Pattern pattern = Pattern.compile("^(.+?)(\\d+)$");
						Matcher m1 = pattern.matcher(n1), m2 = pattern.matcher(n2);
						if (m1.find() && m2.find() && m1.group(1).equals(m2.group(1)))
							return Integer.compare(Integer.parseInt(m1.group(2)), Integer.parseInt(m2.group(2)));
						else
							return n1.compareTo(n2);
					})
					.map(c -> simpleHeader ? c.getVariable().getName() 
							: (c.is(Identifier.class) ? "$" : c.is(Attribute.class) ? "#" : "") + c.getVariable().getName() + "=" + c.getVariable().getDomain()
					).collect(joining(","));
				writer.println(headerLine);

			long size;
			Stream<DataPoint> data;
			try (Stream<DataPoint> stream = ds.stream())
			{
				final Spliterator<DataPoint> spliterator = stream.spliterator();
				size = spliterator.estimateSize();
				data = StreamSupport.stream(spliterator, !Utils.SEQUENTIAL);
			
				try (Stream<DataPoint> wrapper = size < Long.MAX_VALUE && size > 0 ? ProgressWindow.of("Writing " + fileName, size, data) : data)
				{
					wrapper.map(dp -> {
						try 
						{
							writer.println(metadata.stream()
									.map(dp::get)
									.map(Object::toString)
									.collect(joining(",")));
							return null;
						}
						catch (RuntimeException e)
						{
							return e;
						}
					}).filter(Objects::nonNull)
					.findAny()
					.ifPresent(e -> { throw e; });
				}
			}
			
			LOGGER.info("Finished writing csv file in " + fileName);
			return true;
		}
		catch (RuntimeException | FileNotFoundException e)
		{
			LOGGER.error("Error writing csv file " + fileName, e);
			return false;
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
