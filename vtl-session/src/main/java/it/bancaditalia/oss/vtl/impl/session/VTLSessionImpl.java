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

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getLocalConfigurationObject;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.newConfiguration;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.withConfig;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.ref.SoftReference;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

import it.bancaditalia.oss.vtl.config.VTLConfiguration;
import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.engine.RulesetStatement;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
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
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.Utils;

public class VTLSessionImpl implements VTLSession
{
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLSessionImpl.class);
	
	private final VTLConfiguration configuration;
	private final Map<VTLAlias, SoftReference<VTLValue>> cache = new ConcurrentHashMap<>();
	private final Map<VTLAlias, SoftReference<VTLValueMetadata>> metacache = new ConcurrentHashMap<>();
	private final Map<VTLAlias, ReentrantLock> cacheLocks = new ConcurrentHashMap<>();
	private final Map<Class<?>, Map<Transformation, ?>> holders = new ConcurrentHashMap<>();
	private final Map<VTLAlias, Statement> rules = new ConcurrentHashMap<>();

	private final Engine[] engine = new Engine[1];
	private final Environment[][] environments = new Environment[1][];
	private final MetadataRepository[] repository = new MetadataRepository[1];

	private String code;

	public VTLSessionImpl() throws IOException, ClassNotFoundException
	{
		this("");
	}
	
	public VTLSessionImpl(String code) throws IOException, ClassNotFoundException
	{
		this(new StringReader(code), null);
	}

	// The reader is always closed on returning
	public VTLSessionImpl(Reader reader) throws IOException, ClassNotFoundException
	{
		this(reader, null);
	}

	// The reader is always closed on returning
	public VTLSessionImpl(Reader reader, VTLConfiguration configuration) throws IOException, ClassNotFoundException
	{
		try
		{
			StringWriter writer = new StringWriter();
			char[] buffer = new char[1048576];
			int read = 0;
			while ((read = reader.read(buffer)) >= 0)
				writer.write(buffer, 0, read);
			writer.flush();
		
			this.configuration = configuration != null ? new VTLConfiguration(configuration) : newConfiguration();
			this.code = writer.toString();
			
			LOGGER.info("Created new VTL session.");
		}
		finally
		{
			reader.close();
		}
	}

	@Override
	public VTLValue resolve(VTLAlias alias)
	{
		LOGGER.info("Retrieving value for {}", alias);

		Optional<Statement> rule = getRule(alias);
		if (rule.filter(DMLStatement.class::isInstance).isPresent())
		{
			DMLStatement statement = (DMLStatement) rule.get();
			if (statement.isCacheable())
				return cacheHelper(alias, cache, n -> acquireResult(statement, n));
			else
				return acquireResult(statement, alias);
		}
		else
			return cacheHelper(alias, cache, n -> acquireValue(alias, (e, a) -> e.getValue(getRepository(), a))
					.orElseThrow(() -> buildUnboundException(alias, "resolve")));
	}

	@Override
	public VTLValueMetadata getMetadata(VTLAlias alias)
	{
		VTLValueMetadata definedStructure = cacheHelper(alias, metacache, n -> getRepository().getMetadata(alias).orElse(null));
		if (definedStructure != null)
			return definedStructure;
		else
			LOGGER.info("No metadata available for {} in {}.", alias, getRepository().getClass().getSimpleName());
		
		Optional<Statement> rule = getRule(alias);
		if (rule.filter(DMLStatement.class::isInstance).isPresent())
		{
			DMLStatement statement = (DMLStatement) rule.get();
			if (statement.isCacheable())
				return cacheHelper(alias, metacache, n -> statement.getMetadata(this));
			else
				return statement.getMetadata(this);
		}
		else
			return cacheHelper(alias, metacache, n -> acquireValue(n, Environment::getMetadata)
					.orElseThrow(() -> buildUnboundException(alias, "getMetadata")));
	}

	private VTLUnboundAliasException buildUnboundException(VTLAlias alias, String op)
	{
		return new VTLUnboundAliasException(alias);
	}
	
	public String getOriginalCode()
	{
		return code;
	}
	
	private <T extends RuleSet> T findRuleset(VTLAlias alias, Class<T> c)
	{
		return getRule(alias)
				.filter(RulesetStatement.class::isInstance)
				.map(RulesetStatement.class::cast)
				.map(RulesetStatement::getRuleSet)
				.filter(c::isInstance)
				.map(c::cast)
				.orElseThrow(() -> new VTLUndefinedObjectException("Hierarchical ruleset", alias));
	}
	
	@Override
	public DataPointRuleSet findDatapointRuleset(VTLAlias alias)
	{
		return getRepository().getDataPointRuleset(alias).orElseGet(() -> findRuleset(alias, DataPointRuleSet.class));
	}
	
	@Override
	public HierarchicalRuleSet findHierarchicalRuleset(VTLAlias alias)
	{
		return getRepository().getHierarchyRuleset(alias).orElseGet(() -> findRuleset(alias, HierarchicalRuleSet.class));
	}
	
	@Override
	public void persist(VTLValue value, VTLAlias alias)
	{
		try
		{
			boolean saved = Arrays.stream(getEnvironments())
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

		for (Environment env: getEnvironments())
		{
			Optional<T> entry = mapper.apply(env, alias);
			if (entry.isPresent())
			{
				T result = entry.get();
				if (result instanceof DataSet && ((DataSet) result).isCacheable())
					result = (T) new CachedDataSet(this, alias, (DataSet) result);

				LOGGER.info("Found {} in {} ", alias, env.getClass().getSimpleName());
				return Optional.of(result);
			}
			else
				LOGGER.warn("Environment {} doesn't contain {}", env.getClass().getSimpleName(), alias);
		}
		
		return Optional.empty();
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
		engine[0] = null;
		environments[0] = null;
		repository[0] = null;
		
		List<Statement> rules = getEngine().parseRules(code).collect(toList());
		Map<VTLAlias, Statement> newRules = new HashMap<>(); 
		for (Statement rule: rules)
		{
			if (newRules.putIfAbsent(rule.getAlias(), rule) != null)
				throw new IllegalStateException("Object " + rule.getAlias() + " was already defined");
			LOGGER.info("Added a VTL rule with alias {}", rule.getAlias());
		}

		cache.clear();
		metacache.clear();
		cacheLocks.clear();
		holders.clear();
		this.rules.clear();
		this.rules.putAll(newRules);

		Map<DMLStatement, VTLValueMetadata> statements = rules.stream()
			.filter(DMLStatement.class::isInstance)
			.map(DMLStatement.class::cast)
			.collect(toMapWithValues(s -> getMetadata(s.getAlias())));

		LOGGER.info("Compiled {} statements.", statements.size());
		return statements;
	}

	public Map<VTLAlias, String> getStatements()
	{
		Map<VTLAlias, String> result = new LinkedHashMap<>();
		for (Statement rule: getRules())
			result.put(rule.getAlias(), rule.toString());
		return result;
	}

	public List<String> getNodes()
	{
		return rules.values().stream()
				.filter(DMLStatement.class::isInstance)
				.map(DMLStatement.class::cast)
				.flatMap(statement -> Stream.concat(Stream.of(statement.getAlias().toString()), statement.getTerminals().stream().map(LeafTransformation::getText)))
				.distinct()
				.collect(toList());
	}

	public List<List<String>> getTopology()
	{
		List<List<String>> result = Arrays.asList(new ArrayList<>(), new ArrayList<>());

		rules.values().stream()
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
	public MetadataRepository getRepository()
	{
		return getObjectFromConfig(repository, VTLConfiguration::getMetadataRepository);
	}

	@Override
	public Environment[] getEnvironments()
	{
		return getObjectFromConfig(environments, VTLConfiguration::getEnvironments);
	}

	@Override
	public Engine getEngine()
	{
		return getObjectFromConfig(engine, VTLConfiguration::getEngine);
	}
	
	private <T> T getObjectFromConfig(T[] cache, SerFunction<VTLConfiguration, T> what)
	{
		T cached = cache[0];
		if (cached != null)
			return cached;
		cache[0] = withConfig(configuration, () -> getLocalConfigurationObject(what));
		LOGGER.debug("Session is using {}", cache[0]);
		return cache[0];
	}
	
	@Override
	public void updateCode(String code)
	{
		this.code = code;
	}

	@Override
	public List<Statement> getRules()
	{
		return new ArrayList<>(rules.values());
	}
	
	@Override
	public Optional<Statement> getRule(VTLAlias alias)
	{
		return Optional.ofNullable(rules.get(alias));
	}

	@Override
	public VTLConfiguration getConfiguration()
	{
		return configuration;
	}
}
