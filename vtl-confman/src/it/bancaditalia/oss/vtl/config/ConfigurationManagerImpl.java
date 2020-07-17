package it.bancaditalia.oss.vtl.config;

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.VTLProperty.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.VTLProperty.SESSION_IMPLEMENTATION;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class ConfigurationManagerImpl implements ConfigurationManager
{
	private final MetadataRepository metadataRepositoryInstance;
	private final Class<? extends VTLSession> sessionClass;
	
	public ConfigurationManagerImpl() throws InstantiationException, IllegalAccessException, ClassNotFoundException 
	{
		metadataRepositoryInstance = Class.forName(METADATA_REPOSITORY.getValue()).asSubclass(MetadataRepository.class).newInstance();
		sessionClass = Class.forName(SESSION_IMPLEMENTATION.getValue()).asSubclass(VTLSession.class);
	}

	@Override
	public MetadataRepository getMetadataRepositoryInstance()
	{
		return metadataRepositoryInstance;
	}

	@Override
	public VTLSession createSessionInstance()
	{
		try
		{
			return sessionClass.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e)
		{
			throw new VTLNestedException("Error initializing session", e);
		}
	}
}
