package it.bancaditalia.oss.vtl.config;

import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;

public interface ConfigurationManager
{
	public enum VTLProperty {
		METADATA_REPOSITORY("vtl.metadatarepository.class", "it.bancaditalia.oss.vtl.impl.domains.InMemoryMetadataRepository"),
		SESSION_IMPLEMENTATION("vtl.session.implementation.class", "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl");
		
		private final String property;
		private final String defaultValue;
		
		private VTLProperty(String property, String defaultValue)
		{
			this.property = property;
			this.defaultValue = defaultValue;
		}

		public String getValue()
		{
			return System.getProperty(property, defaultValue);
		}

		public String setValue(String newValue)
		{
			return System.setProperty(property, newValue);
		}
	}
	
	public static ConfigurationManager getDefaultFactory()  
	{
		return new ConfigurationManagerFactory().getDefault();
	}
	
	public MetadataRepository getMetadataRepositoryInstance();

	public VTLSession createSessionInstance();
}
