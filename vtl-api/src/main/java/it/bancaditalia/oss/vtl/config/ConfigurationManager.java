package it.bancaditalia.oss.vtl.config;

import it.bancaditalia.oss.vtl.session.MetadataRepository;

public interface ConfigurationManager
{
	public static ConfigurationManager getDefaultFactory()  
	{
		return new ConfigurationManagerFactory().getDefault();
	}
	
	public MetadataRepository getMetadataRepositoryInstance();
}
