package it.bancaditalia.oss.vtl.config;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;

public class ConfigurationManagerFactory
{
	private static final String CONFIG_MANAGER_PROPERTY = "vtl.config.impl.class";
	private static final String CONFIG_MANAGER_DEFAULT = "it.bancaditalia.oss.vtl.config.ConfigurationManagerImpl";

	private final ConfigurationManager config;
	
	public ConfigurationManagerFactory()
	{
		try
		{
			config = Class.forName(System.getProperty(CONFIG_MANAGER_PROPERTY, CONFIG_MANAGER_DEFAULT))
					.asSubclass(ConfigurationManager.class)
					.newInstance();
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
		{
			throw new VTLNestedException("Error loading configuration", e);
		}
	}
	
	public ConfigurationManager getDefault() 
	{
		return config;
	} 
}
