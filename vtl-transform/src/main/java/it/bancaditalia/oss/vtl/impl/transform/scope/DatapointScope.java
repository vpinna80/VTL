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

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class DatapointScope extends AbstractScope
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(DatapointScope.class); 
	
	private final DataPoint dp;
	private final DataSetMetadata structure;
	private final DataStructureComponent<Identifier, ?, ?> timeId;
	private final MetadataRepository repo;
	
	public DatapointScope(MetadataRepository repo, DataPoint dp, DataSetMetadata structure, DataStructureComponent<Identifier, ?, ?> timeId) 
	{
		this.repo = repo;
		this.dp = dp;
		this.structure = structure;
		this.timeId = timeId;
	}

	@Override
	public boolean contains(VTLAlias alias)
	{
		if (THIS.equals(requireNonNull(alias, "The name to resolve cannot be null.")))
			return true;
		
		return structure.getComponent(alias).isPresent();
	}

	@Override
	public VTLValue resolve(VTLAlias alias) 
	{
		if (THIS.equals(requireNonNull(alias, "The name to resolve cannot be null.")))
			return new StreamWrapperDataSet(structure, Collections.singleton(dp)::stream);
			
		LOGGER.trace("Querying {} for {}:{}", alias, dp.hashCode(), dp);
		return structure.getComponent(alias).map(dp::get).orElseThrow(() -> new VTLUnboundAliasException(alias));
	}
	
	@Override
	public VTLValueMetadata getMetadata(VTLAlias alias)
	{
		if (THIS.equals(requireNonNull(alias, "The name to resolve cannot be null.")))
			return structure;
		
		LOGGER.trace("Querying {} for {}:{}", alias, structure.hashCode(), structure);
		return structure.getComponent(alias).map(DataStructureComponent::getVariable).orElseThrow(() -> new VTLUnboundAliasException(alias));
	}

	@Override
	public DMLStatement getRule(VTLAlias node)
	{
		return null;
	}

	@Override
	public MetadataRepository getRepository()
	{
		return repo;
	}

	public ScalarValue<?, ?, ?, ?> getTimeIdValue()
	{
		return dp.get(timeId);
	}
}
