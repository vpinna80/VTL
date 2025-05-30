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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.CONFIG_MANAGER;
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

/**
 * Used by the application to obtain implementing instances of {@link ConfigurationManager},
 * and by various classes implementing components to register properties.
 * 
 * @author Valentino Pinna
 *
 * @see VTLGeneralProperties#CONFIG_MANAGER
 */
public class ConfigurationManagerFactory
{
	private static final Object CLASS_LOCK = new Object();
	private static final Map<Class<?>, List<VTLProperty>> PROPERTIES = new HashMap<>();
	private static ConfigurationManager INSTANCE = null;
	
	private ConfigurationManagerFactory() {}
	
	/**
	 * Initialize the configuration reading VTL properties 
	 * @param input The reader used to initialize a {@link Properties} object
	 * @throws IOException if the load fails
	 */
	public static void loadConfiguration(Reader input) throws IOException
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
	public static void saveConfiguration(Writer output) throws IOException
	{
		Properties props = new Properties();
		for (VTLGeneralProperties prop: VTLGeneralProperties.values())
			props.setProperty(prop.getName(), prop.getValue());
		
		List<VTLProperty> vtlProps = new ArrayList<>();
		for (String envName: ENVIRONMENT_IMPLEMENTATION.getValues())
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
			vtlProps.addAll(ConfigurationManagerFactory.getSupportedProperties(Class.forName(METADATA_REPOSITORY.getValue(), true, Thread.currentThread().getContextClassLoader())));
		}
		catch (ClassNotFoundException e)
		{
			throw new VTLNestedException("Error loading class " + METADATA_REPOSITORY.getValue(), e);
		}
		
		for (VTLProperty prop: vtlProps)
			props.setProperty(prop.getName(), prop.getValue());

		for (String proxyProp: List.of("http.proxyHost", "http.proxyPort", "https.proxyHost", "https.proxyPort"))
		{
			String proxyValue = System.getProperty(proxyProp);
			if (proxyValue != null)
				props.put(proxyProp, proxyValue);
		}
		
		props.store(output, null);
	}

	/**
	 * @return an application-wide {@link ConfigurationManager} instance.
	 */
	public static ConfigurationManager newManager() 
	{
		if (INSTANCE != null)
			return INSTANCE;

		synchronized (CLASS_LOCK)
		{
			if (INSTANCE != null)
				return INSTANCE;

			INSTANCE = instanceOfClass(CONFIG_MANAGER.getValue(), ConfigurationManager.class, "Error loading configuration");
		}
		
		return INSTANCE;
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

	/**
	 * Called by implementation classes to register exposed {@link VTLProperty VTL properties}.
	 * 
	 * @param implementationClass The calling implementation class which is registering properties.
	 * @param classProperties The {@link List} of properties to register.
	 */
	public static void registerSupportedProperties(Class<?> implementationClass, List<VTLProperty> classProperties)
	{
		PROPERTIES.put(implementationClass, classProperties);
	}

	/**
	 * @see #registerSupportedProperties(Class, List)
	 */
	public static void registerSupportedProperties(Class<?> implementationClass, VTLProperty... classProperties)
	{
		PROPERTIES.put(implementationClass, Arrays.asList(classProperties));
	}
	
	public static <T> T instanceOfClass(String className, Class<T> instanceClass, String errorMsg)
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
}
