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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENGINE_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.METADATA_REPOSITORY;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
	 * Initialize the configuration reading VTL properties from a source.
	 * System properties concerning HTTP/S proxies can also be read from the source.
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
			vtlProps.addAll(getSupportedProperties(tryLoading(envName)));
		
		vtlProps.addAll(getSupportedProperties(tryLoading(GLOBAL.getPropertyValue(METADATA_REPOSITORY))));
		
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
	 * Returns a {@link List} of {@link VTLProperty}s belonging to already loaded VTL {@link Class}es.
	 * Classes referenced from the global VTL configuration will be initialized before retrieving the list.
	 * Errors may occur any of these classes cannot load successfully.
	 * The list may change upon different invocations of this method.
	 * 
	 * @return the list of VTL properties
	 */
	public static List<VTLProperty> listAllRegisteredProperties()
	{
		tryLoading(GLOBAL.getPropertyValue(ENGINE_IMPLEMENTATION));
		tryLoading(GLOBAL.getPropertyValue(METADATA_REPOSITORY));
		GLOBAL.getPropertyValues(ENVIRONMENT_IMPLEMENTATION).forEach(ConfigurationManager::tryLoading);
		return PROPERTIES.values().stream().flatMap(Collection::stream).collect(toList());
	}
	
	/**
	 * Allows you to retrieve the properties registered by the given implementation class.
	 * The class is initialized before attempting to retrieve the list.
	 * The list never changes.
	 * 
	 * @param implementationClass The implementation class to query.
	 * @return The list of exposed properties, empty if the class does not expose any property.
	 */
	public static List<VTLProperty> getSupportedProperties(Class<?> implementationClass)
	{
		if (PROPERTIES.containsKey(implementationClass))
			return PROPERTIES.get(implementationClass);

		String classname = implementationClass.getName();
		tryLoading(classname);

		List<VTLProperty> list = PROPERTIES.get(implementationClass);
		if (list == null)
			PROPERTIES.put(implementationClass, emptyList());
			
		return PROPERTIES.get(implementationClass);
	}

	/**
	 * Query for a specific property by name, if supported by given class.
	 *  
	 * @param implementationClass The implementation class to query.
	 * @param name A case-insensitive name of the queried property.
	 * @return The requested {@link VTLProperty} instance, or an empty {@link Optional} if none was found.
	 */
	public static Optional<VTLProperty> findSupportedProperty(Class<?> implementationClass, String name)
	{
		return getSupportedProperties(implementationClass).stream().filter(p -> p.getName().equalsIgnoreCase(name)).findAny();
	}

	/**
	 * Register properties for a given VTL implementation class.
	 * Generally called in the static initialization block from a given implementation class. 
	 * @param implementationClass the class that is registering properties
	 * @param classProperties the properties to register for that class
	 */
	public static void registerSupportedProperties(Class<?> implementationClass, VTLProperty... classProperties)
	{
		PROPERTIES.put(implementationClass, Arrays.asList(classProperties));
	}
	
	public static <T> T instanceOfClass(String className, Class<T> instanceClass)
	{
		try
		{
			return Class.forName(className, true, currentThread().getContextClassLoader()).asSubclass(instanceClass).getDeclaredConstructor().newInstance();
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e)
		{
			throw new VTLNestedException("Error requesting instance of " + className, e);
		}
	}

	/**
	 * Instantiates a new {@link VTLConfiguration} inheriting all the settings in the current global configuration.
	 * @return the new configuration
	 */
	public static VTLConfiguration newConfiguration()
	{
		return new VTLConfiguration(GLOBAL);
	}

	/**
	 * Gets the value set for a {@link VTLProperty} in the global configuration, or null if the property was never set.
	 * @param property the property for which to retrieve the value
	 * @return the value of the property
	 */
	public static String getGlobalPropertyValue(VTLProperty property)
	{
		return GLOBAL.getPropertyValue(property);
	}
	
	/**
	 * Gets the values set for a {@link VTLProperty} in the global configuration, or null if the property was never set.
	 * @param property the property for which to retrieve the values
	 * @return the list of values of the property
	 */
	public static List<String> getGlobalPropertyValues(VTLProperty property)
	{
		return GLOBAL.getPropertyValues(property);
	}
	
	/**
	 * Sets a value for a {@link VTLProperty} in the global configuration.
	 * @param property the property for which to retrieve the values
	 * @param newValue the new value of the property
	 */
	public static void setGlobalPropertyValue(VTLProperty property, String newValue)
	{
		GLOBAL.setPropertyValue(property, newValue);
	}
	
	/**
	 * Tests whether the global configuration has been set to use BigDecimal instead of double.
	 * @return true if using BigDecimal
	 */
	public static boolean isUseBigDecimal()
	{
		return GLOBAL.isUseBigDecimal();
	}
	
	/**
	 * Executes a block of code in a Thread, setting and then clearing the {@link VTLConfiguration} object in that Thread. 
	 * @param <T> The type of the value returned by the code.
	 * @param config the instance to temporarily set in the current thread's {@link ThreadLocal} 
	 * @param callback The code to execute
	 * @return the return value from the code execution 
	 */
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

	/**
	 * Returns an object obtained by appling a transformation to the {@link VTLConfiguration} instance in the current {@link Thread}.
	 * @param <T> the result class type
	 * @param objectMapper the mapper that processes the configuration
	 * @return the requested object instance
	 * @throws IllegalStateException if no {@link VTLConfiguration} was set as a {@link ThreadLocal} in the current Thread.
	 */
	public static <T> T getLocalConfigurationObject(SerFunction<VTLConfiguration, T> objectMapper)
	{
		VTLConfiguration config = LOCAL.get();
		if (config == null)
			throw new IllegalStateException("Configuration not available outside session initialization");
		
		return objectMapper.apply(config);
	}

	/**
	 * Returns the value of a given property in the {@link VTLConfiguration} instance in the current {@link Thread}.
	 * @param property The property to query
	 * @return the requested property value
	 * @throws IllegalStateException if no {@link VTLConfiguration} was set as a {@link ThreadLocal} in the current Thread.
	 */
	public static String getLocalPropertyValue(VTLProperty property)
	{
		return getLocalConfigurationObject(c -> c.getPropertyValue(property));
	}

	/**
	 * Returns a {@link List} of values of a given property in the {@link VTLConfiguration} instance in the current {@link Thread}.
	 * @param property The property to query
	 * @return the requested property values
	 * @throws IllegalStateException if no {@link VTLConfiguration} was set as a {@link ThreadLocal} in the current Thread.
	 */
	public static List<String> getLocalPropertyValues(VTLProperty property)
	{
		return getLocalConfigurationObject(c -> c.getPropertyValues(property));
	}

	static Class<?> tryLoading(String classname) throws AssertionError
	{
		try 
		{
			return Class.forName(classname, true, Thread.currentThread().getContextClassLoader());
	    } 
		catch (ClassNotFoundException e) 
		{
	        throw new VTLNestedException("Error initializing class " + classname, e);
	    }
	}
}
