package it.bancaditalia.oss.vtl.config;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;

public class VTLPropertyImpl implements VTLProperty
{
	private final String name;
	private final String description;
	private final String placeholder;
	private final boolean required;
	private final boolean multiple;
	private final String defaultValue;

	private String value;
	
	public VTLPropertyImpl(String name, String description, String placeholder, boolean required)
	{
		this.name = name;
		this.description = description;
		this.placeholder = placeholder;
		this.required = required;
		this.multiple = false;
		this.defaultValue = "";
		this.value = null;
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
}