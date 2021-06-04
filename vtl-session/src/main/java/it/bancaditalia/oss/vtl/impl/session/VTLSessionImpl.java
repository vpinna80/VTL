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
package it.bancaditalia.oss.vtl.impl.session;

import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.ref.SoftReference;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundNameException;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.util.Utils;

public class VTLSessionImpl implements VTLSession
{
	private final static Logger LOGGER = LoggerFactory.getLogger(VTLSessionImpl.class);

	private final ConfigurationManager config = ConfigurationManager.getDefault();
	private final Engine engine;
	private final List<? extends Environment> environments;
	private final Workspace workspace;
	private final Map<String, SoftReference<VTLValue>> cache = new ConcurrentHashMap<>();
	private final Map<String, SoftReference<VTLValueMetadata>> metacache = new ConcurrentHashMap<>();
	private final Map<String, ReentrantLock> cacheLocks = new ConcurrentHashMap<>();
	private final MetadataRepository repository;

	public VTLSessionImpl()
	{
		this.repository = config.getMetadataRepository();
		this.engine = config.getEngine();
		this.environments = config.getEnvironments();

		Workspace selectedWorkspace = null;
		for (Environment env: environments)
			if (env instanceof Workspace)
				selectedWorkspace = (Workspace) env;
		this.workspace = Optional.ofNullable(selectedWorkspace).orElseThrow(() -> new IllegalStateException("A workspace environment must be supplied."));
	}

	@Override
	public VTLSessionImpl addStatements(String statements)
	{
		engine.parseRules(statements).forEach(workspace::addRule);
		return this;
	}

	@Override
	public VTLSessionImpl addStatements(Reader reader) throws IOException
	{
		engine.parseRules(reader).forEach(workspace::addRule);
		return this;
	}

	@Override
	public VTLSessionImpl addStatements(InputStream inputStream, Charset charset) throws IOException
	{
		engine.parseRules(inputStream, charset).forEach(workspace::addRule);
		return this;
	}

	@Override
	public VTLSessionImpl addStatements(Path path, Charset charset) throws IOException
	{
		engine.parseRules(path, charset).forEach(workspace::addRule);
		return this;
	}

	@Override
	public VTLValue resolve(String name2)
	{
		final String name;
		if (name2.matches("'.*'"))
			name = name2.replaceAll("'(.*)'", "$1");
		else
			name = name2.toLowerCase();

		Optional<? extends Statement> rule = workspace.getRule(name);
		if (rule.isPresent())
		{
			Statement statement = rule.get();
			if (statement.isCacheable())
				return cacheHelper(name, cache, n -> acquireResult(statement, n));
			else
				return acquireResult(statement, name);
		}
		else
			return cacheHelper(name, cache, n -> acquireValue(name, Environment::getValue)
					.orElseThrow(() -> new VTLUnboundNameException(name)));
	}
	
	@Override
	public VTLValueMetadata getMetadata(String name2)
	{
		final String name;
		if (name2.matches("'.*'"))
			name = name2.replaceAll("'(.*)'", "$1");
		else
			name = name2.toLowerCase();

		Optional<? extends Statement> rule = workspace.getRule(name);
		if (rule.isPresent())
		{
			Statement statement = rule.get();
			if (statement.isCacheable())
				return cacheHelper(name, metacache, n -> statement.getMetadata(this));
			else
				return statement.getMetadata(this);
		}
		else
			return cacheHelper(name, metacache, n -> acquireValue(n, Environment::getValueMetadata)
					.orElseThrow(() -> new VTLUnboundNameException(name)));
	}


	@Override
	public boolean contains(String name2)
	{
		final String name;
		if (name2.matches("'.*'"))
			name = name2.replaceAll("'(.*)'", "$1");
		else
			name = name2.toLowerCase();

		Optional<? extends Statement> rule = workspace.getRule(name);
		if (rule.isPresent())
			return true;
		else
			return cacheHelper(name, metacache, n -> acquireValue(n, Environment::getValueMetadata).orElse(null)) != null;
	}

	private <T> T cacheHelper(final String name, Map<String, SoftReference<T>> cache, Function<? super String, ? extends T> mapper)
	{
		ReentrantLock lock = cacheLocks.computeIfAbsent(name, alias -> new ReentrantLock());
		
		if (lock.isHeldByCurrentThread())
		{
			String cycleNames = Utils.getStream(cacheLocks.entrySet())
				.filter(entryByValue(ReentrantLock::isHeldByCurrentThread))
				.map(Entry::getKey)
				.collect(joining(", "));
			throw new IllegalStateException("Found a cycle between rules " + cycleNames);
		}
		
		try
		{
			lock.lockInterruptibly();

			T result = cache.computeIfAbsent(name, n -> new SoftReference<>(null)).get();
			if (result == null)
			{
				result = mapper.apply(name);
				cache.put(name, new SoftReference<>(result));
			}
			
			return result;
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			throw new VTLNestedException("Program interrupted", e);
		}
		finally
		{
			if (lock.isHeldByCurrentThread())
				lock.unlock();
		}
	}

	private <T> Optional<T> acquireValue(final String name, BiFunction<? super Environment, ? super String, ? extends Optional<T>> mapper)
	{
		LOGGER.info("Resolving value of {}", name);

		Optional<T> maybeResult = environments.stream()
				.map(env -> mapper.apply(env, name))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.findAny()
				.map(result -> {
					if (result instanceof DataSet && ((DataSet) result).isCacheable())
					{
						@SuppressWarnings("unchecked")
						T tempResult = (T) new CachedDataSet(this, name, (DataSet) result);
						return tempResult;
					}
					else
						return result;
				});
		

		LOGGER.trace("Finished resolving {}", name);
		return maybeResult;
	}

	private VTLValue acquireResult(Statement statement, String alias)
	{
		LOGGER.info("Applying {}", statement);

		try
		{
			VTLValue result = statement.eval(this);
			if (result instanceof DataSet && ((DataSet) result).isCacheable())
				result = new CachedDataSet(this, alias, (DataSet) result);

			return result;
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("Caught exception while evaluating " + statement, e);
		}
	}

	@Override
	public List<VTLValueMetadata> compile()
	{
		return workspace.getRules().stream()
				.map(s -> s.getMetadata(this))
				.collect(toList());
	}

	public Map<String, String> getStatements()
	{
		return workspace.getRules().stream()
				.collect(toMap(Statement::getId, Statement::toString, (a, b) -> {
					throw new UnsupportedOperationException();
				}, LinkedHashMap::new));
	}

	public List<String> getNodes()
	{
		return workspace.getRules().stream()
				.flatMap(statement -> Stream.concat(Stream.of(statement.getId()), statement.getTerminals().stream().map(LeafTransformation::getText)))
				.distinct()
				.collect(toList());
	}

	public List<List<String>> getTopology()
	{
		List<List<String>> result = Arrays.asList(new ArrayList<>(), new ArrayList<>());

		workspace.getRules().stream().flatMap(rule -> rule.getTerminals().parallelStream()
				.map(t -> t.getText())
				.map(t -> new SimpleEntry<>(rule.getId(), t)))
				.forEach(entry -> {
					synchronized (result)
					{
						result.get(0).add(0, entry.getKey());
						result.get(1).add(0, entry.getValue());
					}
				});

		return result;
	}

	@Override
	public Statement getRule(String name2)
	{
		final String name;
		if (name2.matches("'.*'"))
			name = name2.replaceAll("'(.*)'", "$1");
		else
			name = name2.toLowerCase();

		return workspace.getRule(name).orElseThrow(() -> new VTLUnboundNameException(name));
	}

	@Override
	public MetadataRepository getRepository()
	{
		return repository;
	}

	@Override
	public Engine getEngine()
	{
		return engine;
	}

	@Override
	public Workspace getWorkspace()
	{
		return workspace;
	}

	@Override
	public Lineage linkLineage(String alias)
	{
		Optional<? extends Statement> rule = workspace.getRule(alias);
		return rule.map(Statement::getLineage).orElse(null);
	}
}
