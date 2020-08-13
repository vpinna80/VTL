package it.bancaditalia.oss.vtl.config;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;

public enum VTLGeneralProperties implements VTLProperty  
{
	CONFIG_MANAGER("vtl.config.impl.class", false, "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl"),
	METADATA_REPOSITORY("vtl.metadatarepository.class", false, "it.bancaditalia.oss.vtl.impl.domains.InMemoryMetadataRepository"),
	ENGINE_IMPLEMENTATION("vtl.engine.implementation.class", false, "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine"),
	SESSION_IMPLEMENTATION("vtl.session.implementation.class", false, "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl"),
	ENVIRONMENT_IMPLEMENTATION("vtl.environment.implementation.classes", true, 
			"it.bancaditalia.oss.vtl.impl.environment.CSVFileEnvironment",
			"it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment",
			"it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl");

	private final String name;
	private final boolean multiple;
	private final String defaultValue;
	
	private String value = null;

	private VTLGeneralProperties(String name, boolean multiple, String... defaultValue)
	{
		this.name = name;
		this.multiple = multiple;
		this.defaultValue = Arrays.stream(defaultValue).collect(joining(","));
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
		return "";
	}

	@Override
	public boolean isMultiple()
	{
		return multiple;
	}
	
	@Override
	public boolean isRequired()
	{
		return true;
	}

	@Override
	public String getPlaceholder()
	{
		return "";
	}
}