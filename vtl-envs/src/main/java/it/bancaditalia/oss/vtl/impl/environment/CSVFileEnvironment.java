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

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoField.SEMESTER_OF_YEAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DAYSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.time.format.SignStyle.NOT_NEGATIVE;
import static java.time.format.TextStyle.NARROW;
import static java.time.format.TextStyle.SHORT;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.DAY_OF_YEAR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;
import static java.util.Objects.isNull;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
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
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
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
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.ProgressWindow;
import it.bancaditalia.oss.vtl.util.Utils;

public class CSVFileEnvironment implements Environment
{
	private static final Pattern TOKEN_PATTERN = Pattern.compile("(?:,|\n|^)(\"(?:(?:\"\")*[^\"]*)*\"|([^\",\n]*)|(?:\n|$))");
	private static final Logger LOGGER = LoggerFactory.getLogger(CSVFileEnvironment.class);
	private static final Map<Pattern, UnaryOperator<DateTimeFormatterBuilder>> PATTERNS = new LinkedHashMap<>();
	private static final Pattern DATE_LITERAL_ELEMENT = Pattern.compile("^([-/ ]|\\\\.)(.*)$");
	private static final String DATE_DOMAIN_PATTERN = "^[Dd][Aa][Tt][Ee]\\[(.*)\\]$";
	private static final String PERIOD_DOMAIN_PATTERN = "^[Tt][Ii][Mm][Ee]_[Pp][Ee][Rr][Ii][Oo][Dd]\\[(.*)\\]$";
	
	static {
		PATTERNS.put(Pattern.compile("^(YYYY)(.*)$"), dtf -> dtf.appendValue(YEAR, 4));
		PATTERNS.put(Pattern.compile("^(YYY)(.*)$"), dtf -> dtf.appendValue(YEAR, 3));
		PATTERNS.put(Pattern.compile("^(YY)(.*)$"), dtf -> dtf.appendValue(YEAR, 2));
		PATTERNS.put(Pattern.compile("^(H)(.*)$"), dtf -> dtf.appendValue(SEMESTER_OF_YEAR, 1));
		PATTERNS.put(Pattern.compile("^(Q)(.*)$"), dtf -> dtf.appendValue(QUARTER_OF_YEAR, 1));
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh]3)(.*)$"), dtf -> dtf.appendText(MONTH_OF_YEAR, SHORT));
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh]1)(.*)$"), dtf -> dtf.appendText(MONTH_OF_YEAR, NARROW));
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy]3)(.*)$"), dtf -> dtf.appendText(DAY_OF_WEEK, SHORT));
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy]1)(.*)$"), dtf -> dtf.appendText(DAY_OF_WEEK, NARROW));
		PATTERNS.put(Pattern.compile("^(MM)(.*)$"), dtf -> dtf.appendValue(MONTH_OF_YEAR, 2));
		PATTERNS.put(Pattern.compile("^(M)(.*)$"), dtf -> dtf.appendValue(MONTH_OF_YEAR, 1, 2, NOT_NEGATIVE));
		PATTERNS.put(Pattern.compile("^(PPP)(.*)$"), dtf -> dtf.appendValue(DAY_OF_YEAR, 3));
		PATTERNS.put(Pattern.compile("^(DD)(.*)$"), dtf -> dtf.appendValue(DAY_OF_MONTH, 2));
		PATTERNS.put(Pattern.compile("^(D)(.*)$"), dtf -> dtf.appendValue(DAY_OF_MONTH, 1, 2, NOT_NEGATIVE));
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
	
	@SuppressWarnings("resource")
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
	
			// Do not close here!
			BufferedReader innerReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8));
			
			// Skip header
			innerReader.readLine();
			
			Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, Boolean> set = new ConcurrentHashMap<>();
			return ProgressWindow.of("Loading CSV", lineCount, Utils.getStream(innerReader.lines()))
				// Skip empty lines
				.filter(line -> !line.trim().isEmpty())
				.map(line -> {
					Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> result = new HashMap<>();

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
							result.put(component, mapValue(component, token, masks.get(component)));
							count++;
						}
						else
							throw new IllegalStateException("Expected value for " + metadata.get(count) + " but the row ended before it:\n" + line);
					if (tokenizer.end() < line.length() - 1)
						LOGGER.warn("Skipped trailing characters in line: " + line.substring(tokenizer.end() + 1));
					
					return result;
				})
				.map(m -> new DataPointBuilder(m).build(structure))
				.peek(dp -> LOGGER.trace("Read: {}", dp))
				.peek(dp -> {
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> values = dp.getValues(Identifier.class);
					Boolean a = set.putIfAbsent(values, true);
					if (a != null)
						throw new IllegalStateException("Identifiers are not unique: " + values);
				}).onClose(() -> {
					try
					{
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
		else if (component.getDomain() instanceof DateDomainSubset || component.getDomain() instanceof TimePeriodDomainSubset)
		{
			// Transform the VTL date mask into a DateTimeFormatter
			DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
			while (!mask.isEmpty())
			{
				boolean found = false;
				for (Pattern pattern: PATTERNS.keySet())
					if (!found)
					{
						Matcher matcher = pattern.matcher(mask);
						if (matcher.find())
						{
							builder = PATTERNS.get(pattern).apply(builder);
							mask = matcher.group(2);
							found = true;
						}
					}
				
				if (!found)
				{
					Matcher matcher = DATE_LITERAL_ELEMENT.matcher(mask);
					if (matcher.find())
					{
						builder = builder.appendLiteral(matcher.group(1).replaceAll("\\\\", ""));
						mask = matcher.group(2);
					}
					else
						throw new IllegalStateException("Unrecognized mask in csv header: " + mask);
				}
			}

			TemporalAccessor parsed = builder.toFormatter().parse(value);
			return component.getDomain() instanceof DateDomainSubset ? new DateValue(parsed) : new TimePeriodValue(parsed);
		}

		throw new IllegalStateException("ValueDomain not implemented in CSV: " + component.getDomain());
	}

	private Entry<ValueDomainSubset<? extends ValueDomain>, String> mapVarType(String typeName)
	{
		MetadataRepository repository = ConfigurationManager.getDefault().getMetadataRepository();
		
		if ("STRING".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(STRINGDS, "");
		else if ("NUMBER".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(NUMBERDS, "");
		else if ("INT".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(INTEGERDS, "");
		else if ("BOOL".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(BOOLEANDS, "");
		else if (typeName.matches(DATE_DOMAIN_PATTERN))
			return new SimpleEntry<>(DATEDS, typeName.replaceAll(DATE_DOMAIN_PATTERN, "$1"));
		else if (typeName.matches(PERIOD_DOMAIN_PATTERN))
			return new SimpleEntry<>(DAYSDS, typeName.replaceAll(PERIOD_DOMAIN_PATTERN, "$1"));
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

			if (domain instanceof DateDomain || domain instanceof TimePeriodDomain)
				masks.put(component, mappedType.getValue());
		}
		
		return new SimpleEntry<>(metadata, masks);
	}
}
