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

import static java.util.stream.Collectors.joining;

import java.util.Arrays;

import it.bancaditalia.oss.vtl.config.VTLProperty;

public class VTLPropertyImpl implements VTLProperty
{
	private final String name;
	private final String description;
	private final String placeholder;
	private final boolean required;
	private final boolean multiple;
	private final String defaultValue;

	private String value;
	private boolean hasValue;
	
	public VTLPropertyImpl(String name, String description, String placeholder, boolean required)
	{
		this.name = name;
		this.description = description;
		this.placeholder = placeholder;
		this.required = required;
		this.multiple = false;
		this.defaultValue = "";
		this.value = null;
		this.hasValue = false;
	}

	public VTLPropertyImpl(String name, String description, String placeholder, boolean required, boolean multiple, String... defaultValue)
	{
		this.name = name;
		this.description = description;
		this.placeholder = placeholder;
		this.required = required;
		this.multiple = multiple;
		this.defaultValue = Arrays.stream(defaultValue).collect(joining(","));
		this.value = null;
		this.hasValue = !this.defaultValue.isEmpty();
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public String getValue()
	{
		return value == null || value.isEmpty() ? System.getProperty(name, defaultValue) : value;
	}

	@Override
	public void setValue(String newValue)
	{
		value = newValue;
		hasValue = true;
	}

	@Override
	public String getDescription()
	{
		return description;
	}

	@Override
	public boolean isMultiple()
	{
		return multiple;
	}

	@Override
	public boolean isRequired()
	{
		return required;
	}

	@Override
	public String getPlaceholder()
	{
		return placeholder;
	}
	
	@Override
	public boolean hasValue()
	{
		return hasValue || (getValue() != null && !getValue().isEmpty());
	}
}