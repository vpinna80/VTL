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
package it.bancaditalia.oss.vtl.impl.environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class WorkspaceImpl implements Workspace
{
	private final static Logger LOGGER = LoggerFactory.getLogger(WorkspaceImpl.class);

	private final Map<String, VTLValue> values = new ConcurrentHashMap<>();
	private final Map<String, Statement> rules = new ConcurrentHashMap<>();
	
	@Override
	public synchronized void addRule(Statement statement)
	{
		if (rules.putIfAbsent(statement.getAlias(), statement) != null)
			throw new IllegalStateException("Object " + statement.getAlias() + " was already defined");
		LOGGER.info("Added a VTL rule with alias {}", statement.getAlias());
	}

	@Override
	public List<Statement> getRules()
	{
		return new ArrayList<>(rules.values());
	}
	
	@Override
	public synchronized boolean contains(String alias)
	{
		return values.containsKey(alias) || rules.containsKey(alias);
	}

	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, String alias)
	{
		return Optional.ofNullable(values.get(alias));
	}
	
	@Override
	public Optional<Statement> getRule(String alias)
	{
		return Optional.ofNullable(rules.get(alias));
	}
}
