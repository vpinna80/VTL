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
package it.bancaditalia.oss.vtl.impl.environment.util;

import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.MONTH_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.QUARTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.SEMESTER_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder.Formatter.YEAR_PERIOD_FORMATTER;
import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLTimePatterns.parseString;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalQuery;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class CSVParseUtils
{
	public static final String DATE_DOMAIN_PATTERN = "^[Dd][Aa][Tt][Ee]\\[(.*)\\]$";
	public static final String BOOLEAN_DOMAIN_PATTERN = "^[Bb][Oo][Oo][Ll](?:[Ee][Aa][Nn])?$";
	public static final String PERIOD_DOMAIN_PATTERN = "^[Tt][Ii][Mm][Ee]_[Pp][Ee][Rr][Ii][Oo][Dd]\\[(.*)\\]$";

	private static final Logger LOGGER = LoggerFactory.getLogger(CSVParseUtils.class);
	private static final Map<DateTimeFormatter, TemporalQuery<? extends PeriodHolder<?>>> FORMATTERS = new HashMap<>();

	static
	{
		FORMATTERS.put(MONTH_PERIOD_FORMATTER.get(), MonthPeriodHolder::new);
		FORMATTERS.put(QUARTER_PERIOD_FORMATTER.get(), QuarterPeriodHolder::new);
		FORMATTERS.put(SEMESTER_PERIOD_FORMATTER.get(), SemesterPeriodHolder::new);
		FORMATTERS.put(YEAR_PERIOD_FORMATTER.get(), YearPeriodHolder::new);
	}

	private static class CSVVar<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>, Serializable
	{
		private static final long serialVersionUID = 1L;

		private final VTLAlias name;
		private final S domain;
		
		private transient int hashCode;

		@SuppressWarnings("unchecked")
		public CSVVar(VTLAlias name, ValueDomainSubset<?, ?> domain)
		{
			this.name = name;
			this.domain = (S) domain;
		}

		@Override
		public VTLAlias getAlias()
		{
			return name;
		}

		@Override
		public S getDomain()
		{
			return domain;
		}
		
		@Override
		public <R1 extends Component> DataStructureComponent<R1, S, D> as(Class<R1> role)
		{
			return new DataStructureComponentImpl<>(role, this);
		}
		
		@Override
		public int hashCode()
		{
			return hashCode == 0 ? hashCode = hashCodeInit() : hashCode;
		}

		public int hashCodeInit()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + domain.hashCode();
			result = prime * result + name.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (obj instanceof Variable)
			{
				Variable<?, ?> var = (Variable<?, ?>) obj;
				return name.equals(var.getAlias()) && domain.equals(var.getDomain());
			}
			
			return false;
		}
	}

	private CSVParseUtils()
	{
		
	}

	public static ScalarValue<?, ?, ?, ?> mapValue(ValueDomainSubset<?, ?> domain, String stringRepresentation, String mask)
	{
		if (stringRepresentation == null || stringRepresentation.isEmpty() || "null".equalsIgnoreCase(stringRepresentation))
			return NullValue.instance(domain);
		else if (domain instanceof StringDomainSubset)
			return domain.cast(StringValue.of(stringRepresentation.matches("^\".*\"$") ? stringRepresentation.substring(1, stringRepresentation.length() - 1) : stringRepresentation));
		else if (domain instanceof DurationDomainSubset)
			return Frequency.valueOf(stringRepresentation.matches("^\".*\"$") ? stringRepresentation.substring(1, stringRepresentation.length() - 1) : stringRepresentation).get();
		else if (domain instanceof IntegerDomainSubset)
			try
			{
				if (stringRepresentation.trim().isEmpty())
					return NullValue.instance(INTEGERDS);
				else
					return IntegerValue.of(Long.parseLong(stringRepresentation.trim()));
			}
			catch (NumberFormatException e)
			{
				LOGGER.error("An Integer was expected but found: " + stringRepresentation);
				throw e;
//				return NullValue.instance(INTEGERDS);
			}
		else if (domain instanceof NumberDomainSubset)
			try
			{
				if (stringRepresentation.trim().isEmpty())
					return NullValue.instance(NUMBERDS);
				else 
					return createNumberValue(stringRepresentation.trim());
			}
			catch (NumberFormatException e)
			{
				LOGGER.error("A Number was expected but found: " + stringRepresentation);
				return NullValue.instance(NUMBERDS);
			}
		else if (domain instanceof BooleanDomainSubset)
			if (stringRepresentation == null || stringRepresentation.trim().isEmpty())
				return NullValue.instance(domain);
			else
				return BooleanValue.of(Boolean.parseBoolean(stringRepresentation.trim()));
		else if (domain instanceof DateDomainSubset)
			return DateValue.of(parseString(stringRepresentation, coalesce(mask, "YYYY-MM-DD")));
		else if (domain instanceof TimePeriodDomainSubset)
		{
			if (mask != null)
				throw new UnsupportedOperationException("A mask for time_period in a CSV file is not supported");
			return stringToTimePeriod(stringRepresentation);
		}
		else if (domain instanceof TimeDomainSubset)
		{
			if (mask != null)
				throw new UnsupportedOperationException("A mask for time in a CSV file is not supported");
			
			return stringToTime(stringRepresentation);
		}
		
		throw new IllegalStateException("ValueDomain not implemented in CSV: " + domain);
	}

	public static ScalarValue<?, ?, ?, ?> stringToTime(String stringRepresentation)
	{
		String[] items = stringRepresentation.split("/", 2);
		ScalarValue<?, ?, ?, ?>[] limits = new TimeValue<?, ?, ?, ?>[2];
		
		if (items.length == 1)
			try
			{
				return DateValue.of(parseString(stringRepresentation, "YYYY-MM-DD"));
			}
			catch (RuntimeException last)
			{
				return stringToTimePeriod(stringRepresentation);
			}
		else
			for (int i = 0; i < 2; i++)
				try
				{
					limits[i] = DateValue.of(parseString(items[i], "YYYY-MM-DD"));
				}
				catch (RuntimeException last)
				{
					last = null;
					for (DateTimeFormatter formatter : FORMATTERS.keySet())
						try
						{
							limits[i] = TimePeriodValue.of(formatter.parse(items[i], FORMATTERS.get(formatter)));
							break;
						}
						catch (RuntimeException e1)
						{
							last = e1;
						}
	
					if (limits[i] == null)
						throw last;
				}
		
		return GenericTimeValue.of((TimeValue<?, ?, ?, ?>) limits[0], (TimeValue<?, ?, ?, ?>) limits[1]);
	}

	public static ScalarValue<?, ?, ?, ?> stringToTimePeriod(final String stringRepresentation)
	{
		DateTimeException last = null;
		for (DateTimeFormatter formatter : FORMATTERS.keySet())
			try
			{
				formatter.parse(stringRepresentation, FORMATTERS.get(formatter));
				return TimePeriodValue.of(FORMATTERS.get(formatter).queryFrom(formatter.parse(stringRepresentation)));
			}
			catch (DateTimeException e)
			{
				last = e;
			}

		throw new VTLNestedException("While parsing time_period " + stringRepresentation, last);
	}

	public static Entry<ValueDomainSubset<?, ?>, String> mapVarType(MetadataRepository repo, String typeName)
	{
		if ("STRING".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(STRINGDS, "");
		else if ("NUMBER".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(NUMBERDS, "");
		else if ("INT".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(INTEGERDS, "");
		else if (typeName.matches(BOOLEAN_DOMAIN_PATTERN))
			return new SimpleEntry<>(BOOLEANDS, "");
		else if (typeName.matches(DATE_DOMAIN_PATTERN))
			return new SimpleEntry<>(DATEDS, typeName.replaceAll(DATE_DOMAIN_PATTERN, "$1"));
		else if ("DATE".equalsIgnoreCase(typeName))
			return new SimpleEntry<>(DATEDS, "YYYY-MM-DD");
		else
		{
			VTLAlias alias = VTLAliasImpl.of(typeName);
			Optional<MetadataRepository> maybeRepo = Optional.ofNullable(repo);
			Optional<ValueDomainSubset<?, ?>> domain = maybeRepo.flatMap(r -> r.getDomain(alias));
			if (maybeRepo.isPresent())
				return new SimpleEntry<>(domain.orElseThrow(() -> new VTLUndefinedObjectException("Domain", alias)), typeName);
		}
	
		throw new VTLException("Unsupported type: " + typeName);
	}

	public static Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> extractMetadata(MetadataRepository repo, String headers[])
	{
		// first argument avoids varargs mismatch
		LOGGER.debug("Processing CSV header: {}{}", "", headers);
		
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
			
			Entry<ValueDomainSubset<?, ?>, String> mappedType = mapVarType(repo, typeName);
			ValueDomainSubset<?, ?> domain = mappedType.getKey();
			DataStructureComponent<?, ?, ?> component;
			Class<? extends Component> role;
			if (cname.startsWith("$"))
				role = Identifier.class;
			else if (cname.startsWith("#"))
				role = Attribute.class;
			else
				role = Measure.class;

			String compName = cname.replaceAll("^[$#]", "");
			if (repo != null)
				component = repo.createTempVariable(VTLAliasImpl.of(compName), domain).as(role);
			else
				component = new CSVVar<>(VTLAliasImpl.of(compName), domain).as(role);
			metadata.add(component);

			if (domain instanceof DateDomain || domain instanceof TimePeriodDomain)
				masks.put(component, mappedType.getValue());
		}
		
		return new SimpleEntry<>(metadata, masks);
	}
}
