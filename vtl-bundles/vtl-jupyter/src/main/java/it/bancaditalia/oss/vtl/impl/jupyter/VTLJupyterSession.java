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
package it.bancaditalia.oss.vtl.impl.jupyter;

import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;

import java.util.AbstractMap.SimpleEntry;
import java.util.Optional;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.impl.session.CachedDataSet;
import it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;

public class VTLJupyterSession extends VTLSessionImpl
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLSessionImpl.class);
	
	public VTLJupyterSession(String code)
	{
		super(code);
	}
	
	@Override
	public VTLValue resolve(String alias)
	{
		LOGGER.info("Retrieving value for {}", alias);

		Optional<DMLStatement> rule = getWorkspace().getRule(alias).map(DMLStatement.class::cast);
		if (rule.isPresent())
			return acquireResult(rule.get(), alias);
		else
			return acquireValue(alias, Environment::getValue)
					.orElseThrow(() -> new VTLUnboundAliasException(alias));
	}
	
	@Override
	public VTLValueMetadata getMetadata(String alias)
	{
		Optional<DMLStatement> rule = getWorkspace().getRule(alias).map(DMLStatement.class::cast);
		if (rule.isPresent())
			return rule.get().getMetadata(this);
		else
			return acquireValue(alias, Environment::getValueMetadata)
					.orElseThrow(() -> new VTLUnboundAliasException(alias));
	}

	@Override
	public boolean contains(String alias)
	{
		Optional<? extends Statement> rule = getWorkspace().getRule(alias);
		if (rule.isPresent())
			return true;
		else
			return acquireValue(alias, Environment::getValueMetadata).isPresent();
	}

	private <T> Optional<T> acquireValue(final String alias, BiFunction<? super Environment, ? super String, ? extends Optional<T>> mapper)
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
	
	private VTLValue acquireResult(DMLStatement statement, String alias)
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
}

