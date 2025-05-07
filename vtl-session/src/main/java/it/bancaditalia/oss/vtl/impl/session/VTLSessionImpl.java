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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.lang.ref.SoftReference;
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
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.RulesetStatement;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.rules.RuleSet;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.util.Utils;

public class VTLSessionImpl implements VTLSession
{
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLSessionImpl.class);
	
	private final Engine engine;
	private final List<Environment> environments;
	private final Workspace workspace;
	private final Map<VTLAlias, SoftReference<VTLValue>> cache = new ConcurrentHashMap<>();
	private final Map<VTLAlias, SoftReference<VTLValueMetadata>> metacache = new ConcurrentHashMap<>();
	private final Map<VTLAlias, ReentrantLock> cacheLocks = new ConcurrentHashMap<>();
	private final MetadataRepository repository;
	private final Map<Class<?>, Map<Transformation, ?>> holders = new ConcurrentHashMap<>();
	private final String code;

	public VTLSessionImpl(String code)
	{
		ConfigurationManager c = ConfigurationManagerFactory.newManager();
		this.repository = c.getMetadataRepository();
		this.engine = c.getEngine();
		this.environments = c.getEnvironments();
		this.workspace = c.createWorkspace();
		this.code = code;
		
		engine.parseRules(code).peek(workspace::addRule).collect(toList());
		
		LOGGER.info("Created new VTL session.");
	}

	public VTLSessionImpl(VTLSessionImpl old, String code)
	{
		this(code, old.repository, old.engine, old.environments, ConfigurationManagerFactory.newManager().createWorkspace());
	}

	public VTLSessionImpl(String code, MetadataRepository repository, Engine engine, List<Environment> environments, Workspace workspace)
	{
		this.repository = repository;
		this.engine = engine;
		this.environments = environments;
		this.workspace = workspace;
		this.code = code;
		
		engine.parseRules(code).peek(workspace::addRule).collect(toList());
		
		LOGGER.info("Created new VTL session.");
	}

	@Override
	public VTLValue resolve(VTLAlias alias)
	{
		LOGGER.info("Retrieving value for {}", alias);

		Optional<Statement> rule = workspace.getRule(alias);
		if (rule.filter(DMLStatement.class::isInstance).isPresent())
		{
			DMLStatement statement = (DMLStatement) rule.get();
			if (statement.isCacheable())
				return cacheHelper(alias, cache, n -> acquireResult(statement, n));
			else
				return acquireResult(statement, alias);
		}
		else
			return cacheHelper(alias, cache, n -> acquireValue(alias, (e, a) -> e.getValue(repository, a))
					.orElseThrow(() -> buildUnboundException(alias, "resolve")));
	}

	@Override
	public VTLValueMetadata getMetadata(VTLAlias alias)
	{
		VTLValueMetadata definedStructure = cacheHelper(alias, metacache, n -> repository.getMetadata(alias).orElse(null));
		if (definedStructure != null)
			return definedStructure;
		
		Optional<Statement> rule = workspace.getRule(alias);
		if (rule.filter(DMLStatement.class::isInstance).isPresent())
		{
			DMLStatement statement = (DMLStatement) rule.get();
			if (statement.isCacheable())
				return cacheHelper(alias, metacache, n -> statement.getMetadata(this));
			else
				return statement.getMetadata(this);
		}
		else
			return cacheHelper(alias, metacache, n -> acquireValue(n, Environment::getValueMetadata)
					.orElseThrow(() -> buildUnboundException(alias, "getMetadata")));
	}

	private VTLUnboundAliasException buildUnboundException(VTLAlias alias, String op)
	{
		for (Environment env: environments)
			LOGGER.warn("Environment {} reported empty value for operation {} with {}", env.getClass().getSimpleName(), op, alias);
		return new VTLUnboundAliasException(alias);
	}
	
	@Override
	public boolean contains(VTLAlias alias)
	{
		Optional<? extends Statement> rule = workspace.getRule(alias);
		if (rule.isPresent())
			return true;
		else
			return cacheHelper(alias, metacache, n -> acquireValue(n, Environment::getValueMetadata).orElse(null)) != null;
	}
	
	public String getOriginalCode()
	{
		return code;
	}
	
	private <T extends RuleSet> T findRuleset(VTLAlias alias, Class<T> c)
	{
		return workspace.getRule(alias)
				.filter(RulesetStatement.class::isInstance)
				.map(RulesetStatement.class::cast)
				.map(RulesetStatement::getRuleSet)
				.filter(c::isInstance)
				.map(c::cast)
				.orElseThrow(() -> new VTLUnboundAliasException(alias));
	}
	
	@Override
	public DataPointRuleSet findDatapointRuleset(VTLAlias alias)
	{
		return findRuleset(alias, DataPointRuleSet.class);
	}
	
	@Override
	public HierarchicalRuleSet findHierarchicalRuleset(VTLAlias alias)
	{
		return findRuleset(alias, HierarchicalRuleSet.class);
	}
	
	@Override
	public void persist(VTLValue value, VTLAlias alias)
	{
		try
		{
			boolean saved = getEnvironments().stream()
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

	private <T> T cacheHelper(VTLAlias alias, Map<VTLAlias, SoftReference<T>> cache, Function<VTLAlias, T> mapper)
	{
		ReentrantLock lock = cacheLocks.computeIfAbsent(alias, a -> new ReentrantLock());
		
		if (lock.isHeldByCurrentThread())
		{
			String cycleNames = Utils.getStream(cacheLocks.entrySet())
				.filter(entryByValue(ReentrantLock::isHeldByCurrentThread))
				.map(Entry::getKey)
				.map(VTLAlias::toString)
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
	
	private <T> Optional<T> acquireValue(VTLAlias alias, BiFunction<Environment, VTLAlias, Optional<T>> mapper)
	{
		LOGGER.info("Resolving value of {}", alias);

		Optional<T> maybeResult = getEnvironments().stream()
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

	private VTLValue acquireResult(DMLStatement statement, VTLAlias alias)
	{
		LOGGER.info("Applying {}", statement);

		try
		{
			VTLValue result = statement.eval(this);
			if (result.isDataSet() && ((DataSet) result).isCacheable())
				result = new CachedDataSet(this, alias, (DataSet) result);

			return result;
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("Caught exception while evaluating " + statement, e);
		}
	}

	@Override
	public Map<DMLStatement, VTLValueMetadata> compile()
	{
		Map<DMLStatement, VTLValueMetadata> statements = workspace.getRules().stream()
				.filter(DMLStatement.class::isInstance)
				.map(DMLStatement.class::cast)
				.collect(toMapWithValues(s -> getMetadata(s.getAlias())));

		LOGGER.info("Compiled {} statements.", statements.size());
		return statements;
	}

	public Map<VTLAlias, String> getStatements()
	{
		return workspace.getRules().stream()
				.collect(toMap(Statement::getAlias, Statement::toString, (a, b) -> {
					throw new UnsupportedOperationException();
				}, LinkedHashMap::new));
	}

	public List<String> getNodes()
	{
		return workspace.getRules().stream()
				.filter(DMLStatement.class::isInstance)
				.map(DMLStatement.class::cast)
				.flatMap(statement -> Stream.concat(Stream.of(statement.getAlias().toString()), statement.getTerminals().stream().map(LeafTransformation::getText)))
				.distinct()
				.collect(toList());
	}

	public List<List<String>> getTopology()
	{
		List<List<String>> result = Arrays.asList(new ArrayList<>(), new ArrayList<>());

		workspace.getRules().stream()
				.filter(DMLStatement.class::isInstance)
				.map(DMLStatement.class::cast)
				.flatMap(rule -> rule.getTerminals().parallelStream()
				.map(t -> t.getText())
				.map(t -> new SimpleEntry<>(rule.getAlias(), t)))
				.forEach(entry -> {
					synchronized (result)
					{
						result.get(0).add(0, entry.getKey().toString());
						result.get(1).add(0, entry.getValue());
					}
				});

		return result;
	}

	@Override
	public Statement getRule(VTLAlias alias)
	{
		return workspace.getRule(alias).orElseThrow(() -> new VTLUnboundAliasException(alias));
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
	public <T> Map<Transformation, T> getResultHolder(Class<T> type)
	{
		Map<Transformation, ?> holder = holders.get(type);
		if (holder == null)
			holder = holders.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
		
		@SuppressWarnings("unchecked")
		Map<Transformation, T> result = (Map<Transformation, T>) holder;
		return result;
	}

	@Override
	public List<Environment> getEnvironments()
	{
		return environments;
	}
}
