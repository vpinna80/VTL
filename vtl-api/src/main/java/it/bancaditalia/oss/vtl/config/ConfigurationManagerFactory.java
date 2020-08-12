/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.config;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.CONFIG_MANAGER;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;

public class ConfigurationManagerFactory
{
	private static final Object CLASS_LOCK = new Object();
	private static final Map<Class<?>, List<VTLProperty>> PROPERTIES = new HashMap<>();
	private static ConfigurationManager INSTANCE = null;
	
	private ConfigurationManagerFactory() {}
	
	public static ConfigurationManager getInstance() 
	{
		if (INSTANCE != null)
			return INSTANCE;

		synchronized (CLASS_LOCK)
		{
			if (INSTANCE != null)
				return INSTANCE;

			try
			{
				INSTANCE = Class.forName(CONFIG_MANAGER.getValue())
						.asSubclass(ConfigurationManager.class)
						.newInstance();
				return INSTANCE;
			}
			catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
			{
				throw new VTLNestedException("Error loading configuration", e);
			}
		}
	} 


	public static List<VTLProperty> getSupportedProperties(Class<?> implementationClass)
	{
		if (PROPERTIES.containsKey(implementationClass))
			return PROPERTIES.get(implementationClass);
		
		try 
		{
	        Class.forName(implementationClass.getName(), true, implementationClass.getClassLoader());
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

	public static Optional<VTLProperty> findSupportedProperty(Class<?> implementationClass, String name)
	{
		return getSupportedProperties(implementationClass).stream().filter(p -> p.getName().equals(name)).findAny();
	}

	public static void registerSupportedProperties(Class<?> implementationClass, List<VTLProperty> classProperties)
	{
		PROPERTIES.put(implementationClass, classProperties);
	}

	public static void registerSupportedProperties(Class<?> implementationClass, VTLProperty... classProperties)
	{
		PROPERTIES.put(implementationClass, Arrays.asList(classProperties));
	}
}
