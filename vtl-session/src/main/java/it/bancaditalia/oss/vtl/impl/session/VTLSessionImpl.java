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
/*
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

import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.normalizeAlias;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
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
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.util.Utils;

public class VTLSessionImpl implements VTLSession
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(VTLSessionImpl.class);

	private final ConfigurationManager config = ConfigurationManager.getDefault();
	private final Engine engine;
	private final List<? extends Environment> environments;
	private final Workspace workspace;
	private final Map<String, SoftReference<VTLValue>> cache = new ConcurrentHashMap<>();
	private final Map<String, SoftReference<VTLValueMetadata>> metacache = new ConcurrentHashMap<>();
	private final Map<String, ReentrantLock> cacheLocks = new ConcurrentHashMap<>();
	private final MetadataRepository repository;
	private final Map<Class<?>, Map<Transformation, ?>> holders = new ConcurrentHashMap<>();

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
		LOGGER.info("Created new VTL session.");
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
	public VTLValue resolve(String alias)
	{
		LOGGER.info("Retrieving value for {}", alias);
		final String normalizedAlias = normalizeAlias(alias);

		Optional<? extends Statement> rule = workspace.getRule(alias);
		if (rule.isPresent())
		{
			Statement statement = rule.get();
			if (statement.isCacheable())
				return cacheHelper(normalizedAlias, cache, n -> acquireResult(statement, n));
			else
				return acquireResult(statement, normalizedAlias);
		}
		else
			return cacheHelper(normalizedAlias, cache, n -> acquireValue(normalizedAlias, Environment::getValue)
					.orElseThrow(() -> new VTLUnboundAliasException(normalizedAlias)));
	}
	
	@Override
	public VTLValueMetadata getMetadata(String alias)
	{
		final String normalizedAlias = normalizeAlias(alias);

		VTLValueMetadata definedStructure = cacheHelper(normalizedAlias, metacache, n -> repository.getStructure(normalizedAlias));
		if (definedStructure != null)
			return definedStructure;
		
		Optional<? extends Statement> rule = workspace.getRule(alias);
		if (rule.isPresent())
		{
			Statement statement = rule.get();
			if (statement.isCacheable())
				return cacheHelper(normalizedAlias, metacache, n -> statement.getMetadata(this));
			else
				return statement.getMetadata(this);
		}
		else
			return cacheHelper(normalizedAlias, metacache, n -> acquireValue(n, Environment::getValueMetadata)
					.orElseThrow(() -> new VTLUnboundAliasException(normalizedAlias)));
	}

	@Override
	public boolean contains(String alias)
	{
		final String normalizedAlias = normalizeAlias(alias);

		Optional<? extends Statement> rule = workspace.getRule(normalizedAlias);
		if (rule.isPresent())
			return true;
		else
			return cacheHelper(normalizedAlias, metacache, n -> acquireValue(n, Environment::getValueMetadata).orElse(null)) != null;
	}
	
	@Override
	public void persist(VTLValue value, String alias)
	{
		try
		{
			boolean saved = environments.stream()
				.map(e -> e.store(value, alias))
				.filter(s -> s)
				.findAny()
				.isPresent();
			
			if (!saved)
				throw new VTLUnboundAliasException(alias);
		}
		catch (RuntimeException e)
		{
			throw new VTLNestedException("Error while saving " + alias, e);
		}
	}

	private <T> T cacheHelper(final String alias, Map<String, SoftReference<T>> cache, Function<? super String, ? extends T> mapper)
	{
		ReentrantLock lock = cacheLocks.computeIfAbsent(alias, a -> new ReentrantLock());
		
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

			T result = cache.computeIfAbsent(alias, n -> new SoftReference<>(null)).get();
			if (result == null)
			{
				result = mapper.apply(alias);
				if (result != null)
					cache.put(alias, new SoftReference<>(result));
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

	private <T> Optional<T> acquireValue(final String alias, BiFunction<? super Environment, ? super String, ? extends Optional<T>> mapper)
	{
		LOGGER.info("Resolving value of {}", alias);

		Optional<T> maybeResult = environments.stream()
				.map(env -> new SimpleEntry<>(env, mapper.apply(env, alias)))
				.filter(entryByValue(Optional::isPresent))
				.map(keepingKey(Optional::get))
				.findAny()
				.map(e -> {
					LOGGER.info("{} is bound to {}", alias, e.getKey().getClass().getSimpleName());
					T result = e.getValue();
					if (result instanceof DataSet && ((DataSet) result).isCacheable())
					{
						@SuppressWarnings("unchecked")
						T tempResult = (T) new CachedDataSet(this, alias, (DataSet) result);
						return tempResult;
					}
					else
						return result;
				});
		

		LOGGER.trace("Finished resolving {}", alias);
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
		List<VTLValueMetadata> statements = workspace.getRules().stream()
				.map(s -> s.getMetadata(this))
				.collect(toList());
		LOGGER.info("Compiled {} statements.", statements.size());
		return statements;
	}

	public Map<String, String> getStatements()
	{
		LinkedHashMap<String, String> statements = workspace.getRules().stream()
				.collect(toMap(Statement::getAlias, Statement::toString, (a, b) -> {
					throw new UnsupportedOperationException();
				}, LinkedHashMap::new));
		LOGGER.info("Compiled {} statements.", statements.size());
		return statements;
	}

	public List<String> getNodes()
	{
		return workspace.getRules().stream()
				.flatMap(statement -> Stream.concat(Stream.of(statement.getAlias()), statement.getTerminals().stream().map(LeafTransformation::getText)))
				.distinct()
				.collect(toList());
	}

	public List<List<String>> getTopology()
	{
		List<List<String>> result = Arrays.asList(new ArrayList<>(), new ArrayList<>());

		workspace.getRules().stream().flatMap(rule -> rule.getTerminals().parallelStream()
				.map(t -> t.getText())
				.map(t -> new SimpleEntry<>(rule.getAlias(), t)))
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
	public Statement getRule(String alias)
	{
		final String normalizedAlias = normalizeAlias(alias);

		return workspace.getRule(normalizedAlias).orElseThrow(() -> new VTLUnboundAliasException(normalizedAlias));
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
	public Optional<Lineage> linkLineage(String alias)
	{
		return workspace.getRule(normalizeAlias(alias))
				.map(Statement::getLineage);
	}

	@Override
	public <T> Map<Transformation, T> getResultHolder(Class<T> type)
	{
		Map<Transformation, ?> holder = holders.get(type);
		if (holder == null)
			holder = holders.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
		
		@SuppressWarnings("unchecked")
		Map<Transformation, T> result = (Map<Transformation, T>) holder;
		return result;
	}
}
