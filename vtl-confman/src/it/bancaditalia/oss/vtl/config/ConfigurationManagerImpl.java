package it.bancaditalia.oss.vtl.config;

import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class ConfigurationManagerImpl implements ConfigurationManager
{
	public static final String METADATA_REPOSITORY_PROPERTY = "vtl.metadatarepository.class";
	public static final String METADATA_REPOSITORY_DEFAULT = "it.bancaditalia.oss.vtl.impl.domains.InMemoryMetadataRepository";
	
	private final MetadataRepository metadataRepositoryInstance;
	
	public ConfigurationManagerImpl() throws InstantiationException, IllegalAccessException, ClassNotFoundException 
	{
		metadataRepositoryInstance = Class.forName(System.getProperty(METADATA_REPOSITORY_PROPERTY, METADATA_REPOSITORY_DEFAULT))
				.asSubclass(MetadataRepository.class)
				.newInstance();
	}

	@Override
	public MetadataRepository getMetadataRepositoryInstance()
	{
		return metadataRepositoryInstance;
	}

}
