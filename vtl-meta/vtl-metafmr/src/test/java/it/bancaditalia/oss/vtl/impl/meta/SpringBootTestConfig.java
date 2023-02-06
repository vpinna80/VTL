package it.bancaditalia.oss.vtl.impl.meta;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.sdmx.api.sdmx.manager.structure.SdmxBeanRetrievalManager;
import io.sdmx.core.sdmx.manager.structure.InMemoryRetrievalManagerFast;

@Configuration
public class SpringBootTestConfig
{
	@Bean
	public SdmxBeanRetrievalManager getSBRM()
	{
		return new InMemoryRetrievalManagerFast();
	}
}
