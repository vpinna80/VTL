package it.bancaditalia.oss.vtl.config;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

/**
 * This enum exposes the main configuration properties for the VTL Engine.
 * 
 * Each property enables you to choose an implementation for each of the modules 
 * of which the VTL Engine is composed.
 * 
 * @author Valentino Pinna
 */
public enum VTLGeneralProperties implements VTLProperty  
{
	/**
	 * This property allows to choose how to manage the VTL configuration.
	 * 
	 * The default value may be changed by using the {@code vtl.config.impl.class} system property. 
	 */
	CONFIG_MANAGER("vtl.config.impl.class", false, "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl"),

	/**
	 * This property allows to choose which {@link MetadataRepository metadata repository} should be used by the Engine.
	 * 
	 * The default value may be changed by setting the {@code vtl.metadatarepository.class} system property. 
	 */
	METADATA_REPOSITORY("vtl.metadatarepository.class", false, "it.bancaditalia.oss.vtl.impl.domains.InMemoryMetadataRepository"),

	/**
	 * This property allows to choose which {@link Engine computing engine} should be used.
	 * 
	 * The default value may be changed by setting the {@code vtl.engine.implementation.class} system property. 
	 */
	ENGINE_IMPLEMENTATION("vtl.engine.implementation.class", false, "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine"),

	/**
	 * This property allows to choose which {@link VTLSession VTL session} should be used by the Engine.
	 * 
	 * The default value may be changed by using the {@code vtl.session.implementation.class} system property. 
	 */
	SESSION_IMPLEMENTATION("vtl.session.implementation.class", false, "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl"),

	/**
	 * This property allows to choose which {@link Environment environments} should be used by the Engine.
	 * 
	 * The default value may be changed by using the {@code vtl.environment.implementation.classes} system property to
	 * a sequence of class names separated by comma. 
	 */
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