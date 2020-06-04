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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.model.data.VTLValue;

public class WorkspaceImpl implements Workspace
{
	private final Map<String, VTLValue>			values		= new ConcurrentHashMap<>();
	private final Map<String, Statement>		rules		= Collections.synchronizedMap(new LinkedHashMap<>());
	
	@Override
	public synchronized void addRule(Statement statement)
	{
		if (rules.containsKey(statement.getId()))
			throw new IllegalStateException("Object " + statement.getId() + " was already defined");
		else
			if (statement.getId().matches("'.*'"))
				rules.put(statement.getId().replaceAll("'(.*)'", "$1"), statement);
			else
				rules.put(statement.getId().toLowerCase(), statement);
		
			
	}

	@Override
	public List<Statement> getRules()
	{
		return new ArrayList<>(rules.values());
	}
	
	@Override
	public synchronized boolean contains(String id)
	{
		return values.containsKey(id) || rules.containsKey(id);
	}

	@Override
	public Optional<? extends VTLValue> getValue(String name)
	{
		return Optional.ofNullable(values.get(name));
	}
	
	@Override
	public Optional<Statement> getRule(String name)
	{
		return Optional.ofNullable(rules.get(name));
	}
}
