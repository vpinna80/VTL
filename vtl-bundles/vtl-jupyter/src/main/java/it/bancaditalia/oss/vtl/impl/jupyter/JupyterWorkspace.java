package it.bancaditalia.oss.vtl.impl.jupyter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl;
import it.bancaditalia.oss.vtl.model.data.VTLValue;

public class JupyterWorkspace implements Workspace
{
	private final static Logger LOGGER = LoggerFactory.getLogger(WorkspaceImpl.class);

	private final Map<String, VTLValue> values = new ConcurrentHashMap<>();
	private final Map<String, Statement> rules = new ConcurrentHashMap<>();
	
	@Override
	public void addRule(Statement statement)
	{
		rules.put(statement.getAlias(), statement);
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
	public Optional<VTLValue> getValue(String alias)
	{
		return Optional.ofNullable(values.get(alias));
	}
	
	@Override
	public Optional<Statement> getRule(String alias)
	{
		return Optional.ofNullable(rules.get(alias));
	}
}
