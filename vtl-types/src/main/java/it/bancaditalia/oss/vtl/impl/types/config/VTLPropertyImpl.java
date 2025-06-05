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
package it.bancaditalia.oss.vtl.impl.types.config;

import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_MULTIPLE;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_PASSWORD;
import static java.util.stream.Collectors.joining;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Set;

import it.bancaditalia.oss.vtl.config.VTLProperty;

public class VTLPropertyImpl implements VTLProperty
{
	private final String name;
	private final String description;
	private final String placeholder;
	private final String defaultValue;
	private final Set<Options> options;
	
	public VTLPropertyImpl(String name, String description, String placeholder, Set<Options> options, String... defaultValue)
	{
		if (options.contains(IS_PASSWORD) && defaultValue.length > 0)
			throw new InvalidParameterException("VTLProperty cannot have a default value if it is a password.");

		if (options.contains(IS_MULTIPLE) && defaultValue.length > 0)
			throw new InvalidParameterException("VTLProperty cannot have multiple values if it is a password.");
		
		this.name = name;
		this.options = options;
		this.description = description;
		this.placeholder = placeholder;

		this.defaultValue = Arrays.stream(defaultValue).collect(joining(","));
	}

	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public Set<Options> getOptions()
	{
		return options;
	}

	@Override
	public String getDescription()
	{
		return description;
	}

	@Override
	public String getPlaceholder()
	{
		return placeholder;
	}
	
	@Override
	public String getDefaultValue()
	{
		return defaultValue;
	}
}