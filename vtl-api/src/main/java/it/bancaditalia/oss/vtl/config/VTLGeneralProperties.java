package it.bancaditalia.oss.vtl.config;

public enum VTLGeneralProperties implements VTLProperty  
{
	CONFIG_MANAGER("vtl.config.impl.class", false, "it.bancaditalia.oss.vtl.config.ConfigurationManagerImpl"),
	METADATA_REPOSITORY("vtl.metadatarepository.class", false, "it.bancaditalia.oss.vtl.impl.domains.InMemoryMetadataRepository"),
	ENGINE_IMPLEMENTATION("vtl.engine.implementation.class", false, "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine"),
	SESSION_IMPLEMENTATION("vtl.session.implementation.class", false, "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl"),
	ENVIRONMENT_IMPLEMENTATION("vtl.environment.implementation.classes", true, 
			"it.bancaditalia.oss.vtl.impl.environment.CSVFileEnvironment",
			"it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment",
			"it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl");

	private final VTLProperty delegate;

	private VTLGeneralProperties(String property, boolean multiple, String... defaultValue)
	{
		this.delegate = new VTLPropertyImpl(property, "", "", true, multiple, defaultValue);
	}

	@Override
	public String getName()
	{
		return delegate.getName();
	}
	
	@Override
	public String getValue()
	{
		return delegate.getValue();
	}

	@Override
	public void setValue(String newValue)
	{
		delegate.setValue(newValue);
	}

	@Override
	public String getDescription()
	{
		return delegate.getDescription();
	}

	@Override
	public boolean isMultiple()
	{
		return delegate.isMultiple();
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