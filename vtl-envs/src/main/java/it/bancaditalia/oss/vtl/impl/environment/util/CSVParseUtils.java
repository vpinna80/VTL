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

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLTimePatterns.parseString;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DAYSDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomain;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomain;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class CSVParseUtils
{
	public static final String DATE_DOMAIN_PATTERN = "^[Dd][Aa][Tt][Ee]\\[(.*)\\]$";
	public static final String BOOLEAN_DOMAIN_PATTERN = "^[Bb][Oo][Oo][Ll](?:[Ee][Aa][Nn])?$";
	public static final String PERIOD_DOMAIN_PATTERN = "^[Tt][Ii][Mm][Ee]_[Pp][Ee][Rr][Ii][Oo][Dd]\\[(.*)\\]$";

	private static final Logger LOGGER = LoggerFactory.getLogger(CSVParseUtils.class);

	private CSVParseUtils()
	{
		
	}

	public static ScalarValue<?, ?, ?, ?> mapValue(DataStructureComponent<?, ?, ?> component, final String stringRepresentation, String mask)
	{
		if (stringRepresentation == null || stringRepresentation.isEmpty())
			return NullValue.instanceFrom(component);
		else if (component.getDomain() instanceof StringDomainSubset)
			return component.getDomain().cast(StringValue.of(stringRepresentation.matches("^\".*\"$") ? stringRepresentation.substring(1, stringRepresentation.length() - 1) : stringRepresentation));
		else if (component.getDomain() instanceof IntegerDomainSubset)
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
		else if (component.getDomain() instanceof NumberDomainSubset)
			try
			{
				if (stringRepresentation.trim().isEmpty())
					return NullValue.instance(NUMBERDS);
				else
					return DoubleValue.of(Double.parseDouble(stringRepresentation));
			}
			catch (NumberFormatException e)
			{
				LOGGER.error("A Number was expected but found: " + stringRepresentation);
				return NullValue.instance(NUMBERDS);
			}
		else if (component.getDomain() instanceof BooleanDomainSubset)
			if (stringRepresentation == null || stringRepresentation.trim().isEmpty())
				return NullValue.instanceFrom(component);
			else
				return BooleanValue.of(Boolean.parseBoolean(stringRepresentation));
		else if (component.getDomain() instanceof DateDomainSubset)
			return DateValue.of(parseString(stringRepresentation, mask)); 
	
		throw new IllegalStateException("ValueDomain not implemented in CSV: " + component.getDomain());
	}

	public static Entry<ValueDomainSubset<?, ?>, String> mapVarType(String typeName)
	{
		MetadataRepository repository = ConfigurationManager.getDefault().getMetadataRepository();
		
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
		else if (typeName.matches(PERIOD_DOMAIN_PATTERN))
			return new SimpleEntry<>(DAYSDS, typeName.replaceAll(PERIOD_DOMAIN_PATTERN, "$1"));
		else if (repository.isDomainDefined(typeName))
			return new SimpleEntry<>(repository.getDomain(typeName), typeName);
	
		throw new VTLException("Unsupported type: " + typeName);
	}

	public static Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> extractMetadata(String headers[])
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
			
			Entry<ValueDomainSubset<?, ?>, String> mappedType = CSVParseUtils.mapVarType(typeName);
			ValueDomainSubset<?, ?> domain = mappedType.getKey();
			DataStructureComponent<?, ?, ?> component;
			Class<? extends ComponentRole> role;
			if (cname.startsWith("$"))
				role = Identifier.class;
			else if (cname.startsWith("#"))
				role = Attribute.class;
			else
				role = Measure.class;

			String normalizedName = cname.replaceAll("^[$#]", "");
			normalizedName = normalizedName.matches("'.*'") ? normalizedName.replaceAll("'(.*)'", "$1") : normalizedName.toLowerCase();

			component = DataStructureComponentImpl.of(normalizedName, role, domain);
			metadata.add(component);

			if (domain instanceof DateDomain || domain instanceof TimePeriodDomain)
				masks.put(component, mappedType.getValue());
		}
		
		return new SimpleEntry<>(metadata, masks);
	}
}
