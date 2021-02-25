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
package it.bancaditalia.oss.vtl.impl.transform.scope;

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundNameException;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class DatapointScope implements TransformationScheme
{
	private static final Logger LOGGER = LoggerFactory.getLogger(DatapointScope.class); 
	private final DataPoint dp;
	private final DataSetMetadata structure;
	private final TransformationScheme parent;
	
	public DatapointScope(DataPoint dp, DataSetMetadata structure, TransformationScheme parent) 
	{
		this.dp = dp;
		this.structure = structure;
		this.parent = parent;
	}

	@Override
	public boolean contains(String alias)
	{
		if (THIS.equals(requireNonNull(alias, "The name to resolve cannot be null.")))
			return true;
		
		return maybeMeta(structure, alias, scalar -> (ScalarValueMetadata<?>) scalar::getDomain)
			.isPresent();
	}

	@Override
	public VTLValue resolve(String alias) 
	{
		LOGGER.trace("Querying {} for {}:{}", alias, dp.hashCode(), dp);
		return maybeMeta(structure, alias, dp::get)
			.orElseThrow(() -> new VTLUnboundNameException(alias));
	}
	
	@Override
	public VTLValueMetadata getMetadata(String alias)
	{
		if (THIS.equals(requireNonNull(alias, "The name to resolve cannot be null.")))
			return structure;
		
		LOGGER.trace("Querying {} for {}:{}", alias, structure.hashCode(), structure);
		return maybeMeta(structure, alias, scalar -> (ScalarValueMetadata<?>) scalar::getDomain)
			.orElseThrow(() -> new VTLUnboundNameException(alias));
	}

	private <T> Optional<T> maybeMeta(Set<DataStructureComponent<?, ?, ?>> components, String alias, Function<? super DataStructureComponent<?, ?, ?>, T> mapper)
	{
		Predicate<? super DataStructureComponent<?, ?, ?>> filter = Objects.requireNonNull(alias, "The name to resolve cannot be null.").matches("'.*'")
				? c -> c.getName().equals(alias.replaceAll("'(.*)'", "$1"))
				: c -> c.getName().equalsIgnoreCase(alias);

		Optional<T> result = Utils.getStream(components)
			.filter(filter)
			.findAny()
			.map(mapper);
		LOGGER.trace("Computed {} for {}:{}", alias, components.hashCode(), components);
		return result;
	}

	@Override
	public Statement getRule(String node)
	{
		return parent.getRule(node);
	}

	@Override
	public MetadataRepository getRepository()
	{
		throw new UnsupportedOperationException();
	}
}
