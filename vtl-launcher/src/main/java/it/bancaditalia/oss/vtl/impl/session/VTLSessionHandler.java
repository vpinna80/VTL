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
package it.bancaditalia.oss.vtl.impl.session;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.EngineFactory;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.environment.EnvironmentFactory;
import it.bancaditalia.oss.vtl.impl.session.exceptions.VTLSessionException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.util.Paginator;

public class VTLSessionHandler
{
	private static final String JAVA_ENGINE = "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine";
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLSessionHandler.class);
	private static final Map<String, VTLSession> sessions = new HashMap<>();

	public static VTLSession getSession(String sessionID) throws VTLSessionException
	{
		if(sessions.containsKey(sessionID)) {
			return sessions.get(sessionID);
		}
		
		Map<String, Object> envNames = new HashMap<>();
		envNames.put("it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl", null);
		envNames.put("it.bancaditalia.oss.vtl.impl.environment.CSVFileEnvironment", null);
		envNames.put("it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment", null);
		if(!System.getProperty("NO_R", "false").equalsIgnoreCase("true")){
			envNames.put("it.bancaditalia.oss.vtl.impl.environment.REnvironment", null);
		}
		List<Environment> envs = new ArrayList<>();

		try
		{
			for (Entry<String, Object> entry : envNames.entrySet())
			{
				LOGGER.info("Creating environment " + entry.getKey() + "...");
				Optional<Environment> result = Optional.empty();
				for (EnvironmentFactory factory : EnvironmentFactory.getInstances())
				{
					result = factory.createEnvironment(entry.getKey(), entry.getValue());
					if (result.isPresent())
					{
						envs.add(result.get());
						break;
					}
				}

				if (!result.isPresent())
					throw new ClassNotFoundException("No factory for environment " + entry.getKey());
			}
		}
		catch (Exception e)
		{
			LOGGER.error("Error creating environments", e);
			throw new VTLSessionException("Error creating environments", e);
		}

		return getSession(sessionID, envs);
	}

	public static VTLSession getSession(String sessionID, List<Environment> envs) throws VTLSessionException
	{
		try
		{
			LOGGER.info("Creating java engine...");
			for (EngineFactory factory : EngineFactory.getInstances())
			{
				Optional<Engine> result = factory.createEngine(JAVA_ENGINE, new Object[0]);
				if (result.isPresent())
					return getSession(sessionID, envs, result.get());
			}

			throw new ClassNotFoundException("No factory for engine " + JAVA_ENGINE);
		}
		catch (Exception e)
		{
			LOGGER.error("Error retrieving session", e);
			throw new VTLSessionException("Error retrieving session", e);
		}
	}

	public static VTLSession getSession(String sessionID, List<Environment> envs, Engine engine) // throws VTLSessionException
	{
		return sessions.computeIfAbsent(sessionID, id -> new VTLSessionImpl(envs, engine));
	}

	public static List<String> getSessions()
	{
		return new ArrayList<>(sessions.keySet());
	}

	public static void killSession(String sessionID)
	{
		sessions.remove(sessionID);
	}

	public static VTLSession addStatements(String session, String statements) throws VTLSessionException
	{
		return getSession(session).addStatements(statements);
	}

	public static Map<String, List<Object>> evalNode(String session, String node) throws VTLSessionException
	{
		try
		{
			checkSession(session);

			final VTLValue eval = getSession(session).resolve(node);
	
			if (eval instanceof ScalarValue)
				return singletonMap("Scalar", singletonList(((ScalarValue<?, ?, ?>) eval).get()));
			else if (eval instanceof DataSet)
				try (Paginator pager = new Paginator((DataSet) eval))
				{
					return pager.more(-1);
				}
			else
				throw new UnsupportedOperationException("Unsupported value class: " + eval.getClass().getSimpleName());
		}
		catch (Exception e)
		{
			LOGGER.error("Error evaluating node", e);
			throw new VTLSessionException("Error evaluating node", e);
		}
	}

	private static void checkSession(String session) throws VTLSessionException
	{
		try
		{
			if (!sessions.containsKey(session))
				throw new VTLSessionException("Session '" + session + "' does not exist!");
		}
		catch (Exception e)
		{
			LOGGER.error("Error checking session", e);
			throw new VTLSessionException("Error checking session", e);
		}
	}

	public static void compile(String session) throws VTLSessionException
	{
		LOGGER.info("Starting compilation...");

		try
		{
			checkSession(session);
			
			getSession(session).compile();
		}
		catch (Exception e)
		{
			LOGGER.error("Error in compilation", e);
			throw new VTLSessionException("Error in compilation", e);
		}
	}

	public static Map<String, List<String>> getNodeStructure(String session, String node) throws VTLSessionException
	{
		try
		{
			checkSession(session);
	
			final VTLValue structure = getSession(session).resolve(node);
	
			if (structure instanceof ScalarValue)
				return Arrays.stream(new String[] { "IDENTIFIERS", "MEASURES", "ATTRIBUTES" }).collect(toConcurrentMap(identity(), n -> Collections.emptyList()));
			else
				return ((DataSet) structure).getComponents().stream()
						.collect(groupingBy(VTLSessionHandler::grouper, mapping(DataStructureComponent::getName, toList())));
		}
		catch (Exception e)
		{
			LOGGER.error("Error retrieving structure", e);
			throw new VTLSessionException("Error retrieving structure", e);
		}
	}

	private static String grouper(DataStructureComponent<?, ?, ?> component)
	{
		return component.is(Attribute.class) ? "ATTRIBUTES" : component.is(Measure.class) ? "MEASURES" : "IDENTIFIERS";
	}

	public static Map<String, String> getStatements(String session) throws VTLSessionException
	{
		try
		{
			checkSession(session);

			return ((VTLSessionImpl) getSession(session)).getStatements();
		}
		catch (Exception e)
		{
			LOGGER.error("Error retrieving statements", e);
			throw new VTLSessionException("Error retrieving statements", e);
		}
	}

	public static String getCode(String session) throws VTLSessionException
	{
		try
		{
			checkSession(session);
	
			return ((VTLSessionImpl) getSession(session)).getStatements().values().stream().collect(joining(";\n", "", ";\n"));
		}
		catch (Exception e)
		{
			LOGGER.error("Error retrieving codes", e);
			throw new VTLSessionException("Error retrieving codes", e);
		}
	}

	public static List<String> getNodes(String session) throws VTLSessionException
	{
		try
		{
			checkSession(session);
			
			return ((VTLSessionImpl) getSession(session)).getNodes();
		}
		catch (Exception e)
		{
			LOGGER.error("Error retrieving nodes", e);
			throw new VTLSessionException("Error retrieving nodes", e);
		}
	}

	public static List<List<String>> getTopology(String session) throws VTLSessionException
	{
		try
		{
			checkSession(session);
	
			return ((VTLSessionImpl) getSession(session)).getTopology();
		}
		catch (Exception e)
		{
			LOGGER.error("Error retrieving topology", e);
			throw new VTLSessionException("Error retrieving topology", e);
		}
	}
}
