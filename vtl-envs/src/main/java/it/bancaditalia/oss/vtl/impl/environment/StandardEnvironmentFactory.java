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
package it.bancaditalia.oss.vtl.impl.environment;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.rosuda.JRI.Rengine;

import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.environment.EnvironmentFactory;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;

public class StandardEnvironmentFactory implements EnvironmentFactory
{
	private static Map<Class<? extends Environment>, Class<?>[]> SUPPORTED = new HashMap<>();
	
	static {
		SUPPORTED.put(WorkspaceImpl.class, new Class[0]);
		SUPPORTED.put(SDMXEnvironment.class, new Class[0]);
		SUPPORTED.put(REnvironment.class, new Class[] { Rengine.class });
		SUPPORTED.put(LocalSDMXEnvironment.class, new Class[0]);
		SUPPORTED.put(CSVFileEnvironment.class, new Class[0]);
	}
	
	@Override
	public Optional<Environment> createEnvironment(String classname, Object... configuration)
	{
		return Optional.ofNullable(createIfSupported(classname, configuration));
	}
	
	private Environment createIfSupported(String classname, Object... configuration)
	{
		for (Entry<Class<? extends Environment>, Class<?>[]> entry: SUPPORTED.entrySet())
		{
			Class<? extends Environment> found = entry.getKey();
			if (found.getCanonicalName().equals(classname))
				try
				{
					return found.getDeclaredConstructor().newInstance().init(configuration);
				}
				catch (ReflectiveOperationException e)
				{
					throw new VTLNestedException("Error loading environment '" + classname + "'", e);
				}
		}
		
		return null;
	}
}
