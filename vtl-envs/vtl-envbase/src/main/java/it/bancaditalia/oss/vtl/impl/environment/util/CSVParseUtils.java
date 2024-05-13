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
import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLTimePatterns.parseString;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class CSVParseUtils
{
	public static final String DATE_DOMAIN_PATTERN = "^[Dd][Aa][Tt][Ee]\\[(.*)\\]$";
	public static final String BOOLEAN_DOMAIN_PATTERN = "^[Bb][Oo][Oo][Ll](?:[Ee][Aa][Nn])?$";
	public static final String PERIOD_DOMAIN_PATTERN = "^[Tt][Ii][Mm][Ee]_[Pp][Ee][Rr][Ii][Oo][Dd]\\[(.*)\\]$";

	private static final Logger LOGGER = LoggerFactory.getLogger(CSVParseUtils.class);

	private static class CSVVar<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements Variable<S, D>, Serializable
	{
		private static final long serialVersionUID = 1L;

		private final String name;
		private final S domain;
		
		private transient int hashCode;

		@SuppressWarnings("unchecked")
		public CSVVar(String name, ValueDomainSubset<?, ?> domain)
		{
			this.name = name;
			this.domain = (S) domain;
		}

		@Override
		public String getName()
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
				return name.equals(var.getName()) && domain.equals(var.getDomain());
			}
			
			return false;
		}
	}

	private CSVParseUtils()
	{
		
	}

	public static ScalarValue<?, ?, ?, ?> mapValue(DataStructureComponent<?, ?, ?> component, final String stringRepresentation, String mask)
	{
		LOGGER.trace("Parsing string value {} for component {} with mask {}", stringRepresentation, component, mask);
		
		if (stringRepresentation == null || stringRepresentation.isEmpty() || "null".equalsIgnoreCase(stringRepresentation))
			return NullValue.instanceFrom(component);
		else if (component.getVariable().getDomain() instanceof StringDomainSubset)
			return component.getVariable().getDomain().cast(StringValue.of(stringRepresentation.matches("^\".*\"$") ? stringRepresentation.substring(1, stringRepresentation.length() - 1) : stringRepresentation));
		else if (component.getVariable().getDomain() instanceof IntegerDomainSubset)
			try
			{
				if (stringRepresentation.trim().isEmpty())
					return NullValue.instance(INTEGERDS);
				else
					return IntegerValue.of(Long.parseLong(stringRepresentation));
			}
			catch (NumberFormatException e)
			{
				LOGGER.error("An Integer was expected but found: " + stringRepresentation);
				return NullValue.instance(INTEGERDS);
			}
		else if (component.getVariable().getDomain() instanceof NumberDomainSubset)
			try
			{
				if (stringRepresentation.trim().isEmpty())
					return NullValue.instance(NUMBERDS);
				else 
					return createNumberValue(stringRepresentation);
			}
			catch (NumberFormatException e)
			{
				LOGGER.error("A Number was expected but found: " + stringRepresentation);
				return NullValue.instance(NUMBERDS);
			}
		else if (component.getVariable().getDomain() instanceof BooleanDomainSubset)
			if (stringRepresentation == null || stringRepresentation.trim().isEmpty())
				return NullValue.instanceFrom(component);
			else
				return BooleanValue.of(Boolean.parseBoolean(stringRepresentation));
		else if (component.getVariable().getDomain() instanceof DateDomainSubset)
			return DateValue.of(parseString(stringRepresentation, mask));
	
		throw new IllegalStateException("ValueDomain not implemented in CSV: " + component.getVariable().getDomain());
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
		else if (repo != null && repo.isDomainDefined(typeName))
			return new SimpleEntry<>(repo.getDomain(typeName), typeName);
	
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
			
			Entry<ValueDomainSubset<?, ?>, String> mappedType = CSVParseUtils.mapVarType(repo, typeName);
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
				component = repo.createTempVariable(compName, domain).as(role);
			else
				component = new CSVVar<>(compName, domain).as(role);
			metadata.add(component);

			if (domain instanceof DateDomain || domain instanceof TimePeriodDomain)
				masks.put(component, mappedType.getValue());
		}
		
		return new SimpleEntry<>(metadata, masks);
	}
}
