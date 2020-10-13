/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.ProgressWindow;
import it.bancaditalia.oss.vtl.util.Utils;

public class CSVFileEnvironment implements Environment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CSVFileEnvironment.class);
	private static final Map<Pattern, String> PATTERNS = new LinkedHashMap<>();
	
	static {
		PATTERNS.put(Pattern.compile("^(YYYY)(.*)$"), "yyyy");
		PATTERNS.put(Pattern.compile("^(YYY)(.*)$"), "yyy");
		PATTERNS.put(Pattern.compile("^(YY)(.*)$"), "yy");
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh]3)(.*)$"), "LLL");
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh]1)(.*)$"), "LLLLL");
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy]3)(.*)$"), "ccc");
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy]1)(.*)$"), "ccccc");
		PATTERNS.put(Pattern.compile("^(MM)(.*)$"), "MM");
		PATTERNS.put(Pattern.compile("^(M)(.*)$"), "M");
		PATTERNS.put(Pattern.compile("^(DD)(.*)$"), "dd");
		PATTERNS.put(Pattern.compile("^(D)(.*)$"), "d");
		PATTERNS.put(Pattern.compile("^([-/ ])(.*)$"), "$1");
	}
	
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

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8)))
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
	
	private Stream<DataPoint> streamFileName(String fileName)
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8)))
		{
			Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> headerInfo = extractMetadata(reader.readLine().split(","));
			List<DataStructureComponent<?, ?, ?>> metadata = headerInfo.getKey();
			Map<DataStructureComponent<?, ?, ?>, String> masks = headerInfo.getValue();
			final DataSetMetadata structure = new DataStructureBuilder(metadata).build();
			long lineCount = reader.lines().count();
			
			LOGGER.info("Reading {}", fileName);
	
			// Do not close!
			@SuppressWarnings("resource")
			BufferedReader innerReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8));
			// Skip header
			innerReader.readLine();
			
			Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, Boolean> set = new ConcurrentHashMap<>();
			
			return ProgressWindow.of("Loading CSV", lineCount, Utils.getStream(innerReader.lines()))
				.filter(l -> !l.trim().isEmpty())
				.map(l -> {
					Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> result = new HashMap<>();
					int beginIndex = 0;
					for (int i = 0; i < metadata.size(); i++)
					{
						DataStructureComponent<?, ?, ?> component = metadata.get(i);
						if (beginIndex >= l.length())
							throw new IllegalStateException("Expected value for " + component + " but row ended before: " + l);
							
						int endIndex = l.indexOf(',', beginIndex);
						if (endIndex < 0)
							endIndex = l.length();
						result.put(component, mapValue(component, l.substring(beginIndex, endIndex), masks.get(component)));
						beginIndex = endIndex + 1;
					}
	
					return result;
				})
				.map(m -> new DataPointBuilder(m).build(structure))
				.peek(dp -> LOGGER.trace("Read: {}", dp))
				.peek(dp -> {
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> values = dp.getValues(Identifier.class);
					Boolean a = set.putIfAbsent(values, true);
					if (a != null)
						throw new IllegalStateException("Identifiers are not unique: " + values);
				});
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + fileName, e);
		}
	}

	private ScalarValue<?, ?, ?> mapValue(DataStructureComponent<?, ?, ?> component, final String value, String mask)
	{
		if (component.getDomain() instanceof StringDomainSubset)
			return new StringValue(value.matches("^\".*\"$") ? value.substring(1, value.length() - 1) : value);
		else if (component.getDomain() instanceof IntegerDomainSubset)
			try
			{
				if (value.trim().isEmpty())
					return NullValue.instance(INTEGERDS);
				else
					return new IntegerValue(Long.parseLong(value));
			}
			catch (NumberFormatException e)
			{
				LOGGER.error("An Integer was expected but found: " + value);
				return NullValue.instance(INTEGERDS);
			}
		else if (component.getDomain() instanceof NumberDomainSubset)
			try
			{
				if (value.trim().isEmpty())
					return NullValue.instance(NUMBERDS);
				else
					return new DoubleValue(Double.parseDouble(value));
			}
			catch (NumberFormatException e)
			{
				LOGGER.error("A Number was expected but found: " + value);
				return NullValue.instance(NUMBERDS);
			}
		else if (component.getDomain() instanceof BooleanDomainSubset)
			return BooleanValue.of(Boolean.parseBoolean(value));
		else if (component.getDomain() instanceof DateDomainSubset)
		{
			StringBuilder maskBuilder = new StringBuilder();
			while (!mask.isEmpty())
			{
				boolean found = false;
				for (Pattern pattern: PATTERNS.keySet())
					if (!found)
					{
						Matcher matcher = pattern.matcher(mask);
						if (matcher.find())
						{
							maskBuilder.append(matcher.replaceFirst(PATTERNS.get(pattern)));
							mask = matcher.group(2);
							found = true;
						}
					}
				
				if (!found)
					throw new IllegalStateException("Unrecognized mask in cast operator: " + mask);
			}

			DateTimeFormatter formatter = new DateTimeFormatterBuilder()
				.appendPattern(maskBuilder.toString())
				.parseDefaulting(MONTH_OF_YEAR, 1)
				.parseDefaulting(DAY_OF_MONTH, 1)
				.parseDefaulting(HOUR_OF_DAY, 0)
	            .parseDefaulting(MINUTE_OF_HOUR, 0)
	            .parseDefaulting(SECOND_OF_MINUTE, 0)
	            .toFormatter();
			return new DateValue(LocalDateTime.parse(value, formatter));
		}

		throw new IllegalStateException("ValueDomain not supported in CSV: " + component.getDomain());
	}

	private Entry<ValueDomainSubset<? extends ValueDomain>, String> mapVarType(String typeName)
	{
		String datePattern = "^[Dd][Aa][Tt][Ee]\\[(.*)\\]$";
		
		MetadataRepository repository = ConfigurationManager.getDefault().getMetadataRepository();
		
		if ("STRING".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(STRINGDS, "");
		else if ("NUMBER".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(NUMBERDS, "");
		else if ("INT".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(INTEGERDS, "");
		else if ("BOOL".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(BOOLEANDS, "");
		else if (typeName.matches(datePattern))
			return new SimpleEntry<>(DATEDS, typeName.replaceAll(datePattern, "$1"));
		else if (repository.isDomainDefined(typeName))
			return new SimpleEntry<>(repository.getDomain(typeName), typeName);

		throw new VTLException("Unsupported type: " + typeName);
	}

	@Override
	public Optional<VTLValueMetadata> getValueMetadata(String name)
	{
		if (!contains(name))
			return Optional.empty();

		String fileName = name.substring(4);

		LOGGER.debug("Looking for csv file '{}'", fileName);

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8)))
		{
			return Optional.of(new DataStructureBuilder(extractMetadata(reader.readLine().split(",")).getKey()).build());
		}
		catch (IOException e)
		{
			throw new VTLNestedException("Exception while reading " + fileName, e);
		}
	}

	private Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> extractMetadata(String headers[]) throws IOException
	{
		List<DataStructureComponent<?, ?, ?>> metadata = new ArrayList<>();
		Map<DataStructureComponent<?, ?, ?>, String> masks = new HashMap<>();
		for (String header: headers)
		{
			String cname, typeName;
			
			if (header.indexOf('=') >= 0)
			{
				cname = header.split("=", 2)[0];
				typeName = header.split("=", 2)[1];
			}
			else
			{
				cname = '$' + header;
				typeName = "String";
			}
			
			Entry<ValueDomainSubset<? extends ValueDomain>, String> mappedType = mapVarType(typeName);
			ValueDomainSubset<? extends ValueDomain> domain = mappedType.getKey();
			Class<? extends Component> role = cname.startsWith("$") ? Identifier.class : cname.startsWith("#") ? Attribute.class : Measure.class;
			cname = cname.replaceAll("^[$#]", "");
			DataStructureComponentImpl<? extends Component, ?, ? extends ValueDomain> component = new DataStructureComponentImpl<>(cname, role, domain);
			metadata.add(component);

			if (domain instanceof DateDomain)
				masks.put(component, mappedType.getValue());
		}
		
		return new SimpleEntry<>(metadata, masks);
	}
	
	/*private static class MemoryMapper implements Spliterator<String>
	{
		private static final long MAXSIZE = Integer.MAX_VALUE; // 64MB
		private static final Logger LOGGER = LoggerFactory.getLogger(MemoryMapper.class);
		
		private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
		private final FileChannel channel;
		private final long size;
		private final CharBuffer out = CharBuffer.allocate(1024 * 1024);
		private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
		private final AtomicInteger counter = new AtomicInteger();
		private final int lines;
		
		private MappedByteBuffer buffer = null;
		private long lastPosition = 0;
		private CoderResult lastDecodeResult = null;
		private int splits = 1;

		@SuppressWarnings("resource")
		public MemoryMapper(String name, int lines) throws IOException
		{
			this.lines = lines;
			this.channel = new RandomAccessFile(name, "r").getChannel();
			size = channel.size();
			fill();
			final Thread producer = new Thread(this::producer, "Reader thread for " + name);
			producer.setDaemon(true);
			producer.start();
		}
		
		private void fill() throws IOException 
		{
			long toRead = size - lastPosition;
			if (buffer != null && buffer.hasRemaining())
			{
				out.clear();
				lastDecodeResult = decoder.decode(buffer, out, false);
				if (lastDecodeResult.isError())
					lastDecodeResult.throwException();
				out.flip();
			}
			else if (toRead > 0)
			{
				if (toRead > MAXSIZE)
					toRead = MAXSIZE;
				
				buffer = channel.map(FileChannel.MapMode.READ_ONLY, lastPosition, toRead);
				lastPosition += toRead;
				LOGGER.trace("Read {} bytes", toRead);
				out.clear();
				lastDecodeResult = decoder.decode(buffer, out, false);
				out.flip();
				if (lastDecodeResult.isError())
					lastDecodeResult.throwException();
			}
			else if (lastPosition >= size)
			{
				out.clear();
				lastDecodeResult = decoder.decode(ByteBuffer.allocate(0), out, true);
				decoder.flush(out);
				if (lastDecodeResult.isError())
					lastDecodeResult.throwException();
				out.flip();
			}
		}
		
		private static int indexOf(char[] array, int beginIndex)
		{
			int i = beginIndex;
			while (i < array.length)
				if (array[i++] == '\n')
					return i - beginIndex;
			
			return -1;
		}

		public void producer()
		{
			char elements[] = out.toString().toCharArray();
			String residual = "";
			int beginIndex = 0;
			
			try
			{
				while (beginIndex < elements.length)
				{
					int nl = indexOf(elements, beginIndex);
					
					if (nl >= 0)
					{
						String element = new String(elements, beginIndex, nl - 1);
						beginIndex += nl;
						if (!residual.isEmpty())
						{
							element = residual + element;
							residual = "";
						}
							
						final int length = element.length();
						if (element.charAt(length - 1) == '\r')
							element = element.substring(0, length - 1);
						//LOGGER.trace("Read line from csv: {}", element);
						queue.put(element);
					}
					else
					{
						fill();
						
						if (out.hasRemaining())
						{
							if (beginIndex < elements.length)
								residual += new String(elements, beginIndex, elements.length - beginIndex);
							elements = out.toString().toCharArray();
							beginIndex = 0;
						}
						else
							queue.put(residual + new String(elements));
					}
				}
			}
			catch (IOException | InterruptedException e)
			{
				LOGGER.error("Error while reading CSV: ", e);
				throw new RuntimeException(e);
			}
			
			LOGGER.debug("Finished reading CSV");
		}

		@Override
		public boolean tryAdvance(Consumer<? super String> action)
		{
			if (counter.getAndIncrement() >= lines)
				return false;
			
			try
			{
				action.accept(queue.take());
				return true;
			}
			catch (InterruptedException e)
			{
				return false;
			}
		}
		
		@Override
		public Spliterator<String> trySplit()
		{
			splits++;
			return this;
		}
		
		@Override
		public int characteristics()
		{
			return Spliterator.IMMUTABLE + Spliterator.CONCURRENT + Spliterator.SIZED + Spliterator.NONNULL;
		}

		@Override
		public long estimateSize()
		{
			return (lines - counter.get()) / splits;
		}
	}*/
}
