/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
package it.bancaditalia.oss.vtl.config;

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getGlobalPropertyValue;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getSupportedProperties;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.instanceOfClass;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.newConfiguration;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.tryLoading;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENGINE_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.SESSION_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.USE_BIG_DECIMAL;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

/**
 * Interface for the service used by the application to load and configure
 * components of VTL sessions.
 * 
 * Instances of this interface are provided by {@link ConfigurationManager#newConfiguration()}.
 * 
 * @author Valentino Pinna
 */
public class VTLConfiguration implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLConfiguration.class);
	
	protected final Map<VTLProperty, String> values = new ConcurrentHashMap<>();
	
	/**
	 * Create a deep-copy instance of current configuration
	 * @param source The configuration to be copied
	 */
	public VTLConfiguration(VTLConfiguration source)
	{
		reset(source);
	}

	/**
	 * @param property the property to retrieve
	 * @return The current value for a property definition
	 */
	public String getPropertyValue(VTLProperty property)
	{
		return values.computeIfAbsent(property, k -> getGlobalPropertyValue(property));
	}

	/**
	 * Change the value for the given property to a new value.
	 * @param property the property whose value is to set
	 * @param newValue The new value for the property.
	 */
	public void setPropertyValue(VTLProperty property, Object newValue)
	{
		if (newValue == null)
		{
			values.remove(property);
			LOGGER.debug("Unset configuration property {}", property);
		}
		else
		{
			values.put(property, Objects.toString(newValue));
			LOGGER.debug("Set configuration property {} to {}", property, Objects.toString(newValue));
		}
	}

	/**
	 * Change the value for the given property to a value that is the fully qualified name of the given class.
	 * @param property the property whose value is to set
	 * @param newValue The new value for the property
	 */
	public void setPropertyValue(VTLProperty property, Class<?> newValue)
	{
		setPropertyValue(property, newValue.getName());
	}

	/**
	 * Return all values set for a given {@link VTLProperty}.
	 * @param property the property to inquiry.
	 * @return If the property {@link VTLProperty#isMultiple()}, a {@link List} where the elements 
	 * 		match each of the property's values
	 */
	public List<String> getPropertyValues(VTLProperty property)
	{
		String value = getPropertyValue(property);
		return value == null || value.trim().isEmpty() ? List.of() : property.isMultiple() ? Arrays.asList(value.split(",")) : List.of(value);
	}

	/**
	 * Return all classes whose names are the values set for a given {@link VTLProperty}.
	 * @param property the property to inquiry.
	 * @return If the property {@link VTLProperty#isMultiple()}, a {@link List} where the elements are 
	 * 		classes whose names match each of the property's values
	 */
	public List<Class<?>> getPropertyClasses(VTLProperty property)
	{
		String value = getPropertyValue(property);
		if (value == null || value.trim().isEmpty())
			return List.of();
		List<Class<?>> classes = new ArrayList<>();
		for (String className: property.isMultiple() ? Arrays.asList(value.split(",")) : List.of(value))
			classes.add(tryLoading(className));
		return classes;
	}

	/**
	 * @return The {@link MetadataRepository} instance
	 */
	public MetadataRepository getMetadataRepository()
	{
		return instanceOfClass(getPropertyValue(METADATA_REPOSITORY), MetadataRepository.class);
	}

	/**
	 * @return The {@link Engine} instance
	 */
	public Engine getEngine()
	{
		return instanceOfClass(getPropertyValue(ENGINE_IMPLEMENTATION), Engine.class);
	}

	/**
	 * @return The {@link List} of {@link Environment} instances
	 */
	public Environment[] getEnvironments()
	{
		List<Environment> result = new ArrayList<>();

		List<String> envNames = getPropertyValues(ENVIRONMENT_IMPLEMENTATION);
		for (String envName: envNames)
			result.add(instanceOfClass(envName, Environment.class));

		return result.toArray(Environment[]::new);
	}
	
	/**
	 * @return True if the configuration is set to use {@link BigDecimal} instead of double-precision numbers.
	 */
	public boolean isUseBigDecimal()
	{
		return Boolean.parseBoolean(getPropertyValue(USE_BIG_DECIMAL));
	}
	
	/**
	 * Resets this configuration to the same values as the global configration.
	 * 
	 * NOTE: This method is not thread-safe.
	 */
	public void reset()
	{
		reset(newConfiguration());
	}

	protected VTLConfiguration()
	{

	}

	private void reset(VTLConfiguration source)
	{
		values.clear();
		
		for (VTLGeneralProperties prop: EnumSet.allOf(VTLGeneralProperties.class))
			setPropertyValue(prop, source.getPropertyValue(prop));

		for (VTLGeneralProperties topProp: EnumSet.of(ENVIRONMENT_IMPLEMENTATION, METADATA_REPOSITORY, SESSION_IMPLEMENTATION, ENGINE_IMPLEMENTATION))
			source.getPropertyClasses(topProp).forEach(clazz -> 
				getSupportedProperties(clazz).forEach(prop -> setPropertyValue(prop, source.getPropertyValue(prop))));
	}
}
