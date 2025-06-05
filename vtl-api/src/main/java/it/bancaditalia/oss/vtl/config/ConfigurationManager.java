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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerSupplier;

/**
 * Used by the application to obtain implementing instances of {@link VTLConfiguration},
 * and by various classes implementing components to register properties.
 * 
 * @author Valentino Pinna
 *
 * @see VTLConfiguration
 * @see VTLGeneralProperties
 */
public class ConfigurationManager
{
	private static final Map<Class<?>, List<VTLProperty>> PROPERTIES = new HashMap<>();
	private static final ThreadLocal<VTLConfiguration> LOCAL = new ThreadLocal<>();
	private static final VTLConfiguration GLOBAL = new VTLConfiguration() {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public String getPropertyValue(VTLProperty property)
		{
			String value = values.get(property);
			return value == null || value.trim().isEmpty() ? System.getProperty(property.getName(), property.getDefaultValue()) : value;
		}
	};
	
	private ConfigurationManager() {}
	
	/**
	 * Initialize the configuration reading VTL properties 
	 * @param input The reader used to initialize a {@link Properties} object
	 * @throws IOException if the load fails
	 */
	public static void loadGlobalConfiguration(Reader input) throws IOException
	{
		Properties props = new Properties();
		props.load(input);
		props.forEach((k, v) -> {
			if (k != null && v != null && k instanceof String && ((String) k).startsWith("vtl."))
				System.setProperty(k.toString(), v.toString());
			
			if (List.of("http.proxyHost", "http.proxyPort", "https.proxyHost", "https.proxyPort").contains(k))
			{
				// Set proxy only if not already set (i.e. by command line parameters)
				String proxyValue = System.getProperty(k.toString());
				if (proxyValue == null)
					System.setProperty(k.toString(), v.toString());
			}
		});
	}

	/**
	 * Saves the current configuration to the provided Writer as a list of Java properties.
	 * 
	 * @param output The stream to write the properties to.
	 * @throws IOException if a i/o problem arises while saving properties.
	 * @throws UnsupportedOperationException in case the operation is not supported by this ConfigurationManager.
	 */
	public static void saveGlobalConfiguration(Writer output) throws IOException
	{
		Properties props = new Properties();
		for (VTLGeneralProperties prop: VTLGeneralProperties.values())
			props.setProperty(prop.getName(), GLOBAL.getPropertyValue(prop));
		
		List<VTLProperty> vtlProps = new ArrayList<>();
		for (String envName: GLOBAL.getPropertyValues(ENVIRONMENT_IMPLEMENTATION))
			try
			{
				vtlProps.addAll(getSupportedProperties(Class.forName(envName, true, Thread.currentThread().getContextClassLoader())));
			}
			catch (ClassNotFoundException e)
			{
				throw new VTLNestedException("Error loading class " + envName, e);
			}
		
		try
		{
			vtlProps.addAll(ConfigurationManager.getSupportedProperties(Class.forName(GLOBAL.getPropertyValue(METADATA_REPOSITORY), true, Thread.currentThread().getContextClassLoader())));
		}
		catch (ClassNotFoundException e)
		{
			throw new VTLNestedException("Error loading class " + GLOBAL.getPropertyValue(METADATA_REPOSITORY), e);
		}
		
		for (VTLProperty prop: vtlProps)
			props.setProperty(prop.getName(), GLOBAL.getPropertyValue(prop));

		for (String proxyProp: List.of("http.proxyHost", "http.proxyPort", "https.proxyHost", "https.proxyPort"))
		{
			String proxyValue = System.getProperty(proxyProp);
			if (proxyValue != null)
				props.put(proxyProp, proxyValue);
		}
		
		props.store(output, null);
	}

	/**
	 * Allows you to retrieve the properties registered by the given implementation class.
	 * 
	 * @param implementationClass The implementation class to query.
	 * @return The list of exposed properties, empty if the class does not expose any property.
	 */
	public static List<VTLProperty> getSupportedProperties(Class<?> implementationClass)
	{
		if (PROPERTIES.containsKey(implementationClass))
			return PROPERTIES.get(implementationClass);
		
		try 
		{
	        Class.forName(implementationClass.getName(), true, Thread.currentThread().getContextClassLoader());
	    } 
		catch (ClassNotFoundException e) 
		{
	        throw new AssertionError(e); // Can't happen
	    }

		List<VTLProperty> list = PROPERTIES.get(implementationClass);
		if (list == null)
			PROPERTIES.put(implementationClass, Collections.emptyList());
			
		return PROPERTIES.get(implementationClass);
	}

	/**
	 * Query for a specific property by name, if supported by given class.
	 *  
	 * @param implementationClass The implementation class to query.
	 * @param name The name of the queried property.
	 * @return The requested {@link VTLProperty} instance, or an empty {@link Optional} if none was found.
	 */
	public static Optional<VTLProperty> findSupportedProperty(Class<?> implementationClass, String name)
	{
		return getSupportedProperties(implementationClass).stream().filter(p -> p.getName().equals(name)).findAny();
	}

	public static void registerSupportedProperties(Class<?> implementationClass, VTLProperty... classProperties)
	{
		PROPERTIES.put(implementationClass, Arrays.asList(classProperties));
	}
	
	public static <T> T instanceOfClass(String className, Class<T> instanceClass)
	{
		try
		{
			return Class.forName(className, true, Thread.currentThread().getContextClassLoader()).asSubclass(instanceClass).getDeclaredConstructor().newInstance();
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e)
		{
			throw new VTLNestedException("Error requesting instance of " + className, e);
		}
	}
	
	public static String getGlobalPropertyValue(VTLProperty property)
	{
		return GLOBAL.getPropertyValue(property);
	}
	
	public static List<String> getGlobalPropertyValues(VTLProperty property)
	{
		return GLOBAL.getPropertyValues(property);
	}
	
	public static void setGlobalPropertyValue(VTLProperty property, String newValue)
	{
		GLOBAL.setPropertyValue(property, newValue);
	}
	
	public static boolean isUseBigDecimal()
	{
		return GLOBAL.isUseBigDecimal();
	}
	
	public static <T> T withConfig(VTLConfiguration config, SerSupplier<T> callback)
	{
		try
		{
			LOCAL.set(config);
			return callback.get();
		}
		finally
		{
			LOCAL.remove();
		}
	}

	public static String getLocalPropertyValue(VTLProperty property)
	{
		VTLConfiguration config = LOCAL.get();
		if (config == null)
			throw new IllegalStateException("Configuration not available outside session initialization");
		
		return config.getPropertyValue(property);
	}

	public static <T> T getLocalConfigurationObject(SerFunction<VTLConfiguration, T> objectMapper)
	{
		VTLConfiguration config = LOCAL.get();
		if (config == null)
			throw new IllegalStateException("Configuration not available outside session initialization");
		
		return objectMapper.apply(config);
	}

	public static List<String> getLocalPropertyValues(VTLProperty property)
	{
		VTLConfiguration config = LOCAL.get();
		if (config == null)
			throw new IllegalStateException("Configuration not available outside session initialization");
		
		return config.getPropertyValues(property);
	}

	public static VTLConfiguration newConfiguration() throws ClassNotFoundException
	{
		return new VTLConfiguration(GLOBAL);
	}
}
