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
package it.bancaditalia.oss.vtl.impl.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.EngineFactory;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;

public class StandardEngineFactory implements EngineFactory
{
	private static Map<Class<? extends Engine>, Class<?>[]> SUPPORTED = new HashMap<>();
	
	static 
	{
		SUPPORTED.put(JavaVTLEngine.class, new Class[0]);
	}
	
	@Override
	public Optional<Engine> createEngine(String classname, Object... configuration)
	{
		return Optional.ofNullable(createIfSupported(classname, configuration));
	}
	
	private Engine createIfSupported(String classname, Object... configuration)
	{
		for (Entry<Class<? extends Engine>, Class<?>[]> entry: SUPPORTED.entrySet())
		{
			Class<? extends Engine> found = entry.getKey();
			if (found.getCanonicalName().equals(classname))
				try
				{
					return found.getDeclaredConstructor().newInstance().init(configuration);
				}
				catch (ReflectiveOperationException e)
				{
					throw new VTLNestedException("Error loading engine '" + classname + "'", e);
				}
		}
		
		return null;
	}
}
