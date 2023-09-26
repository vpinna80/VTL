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

import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.normalizeAlias;

import java.util.Optional;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class ThisScope extends AbstractScope
{
	private static final long serialVersionUID = 1L;
	public static final String THIS = "$$THIS";
	
	private final DataSet thisValue;
	private final DataSetMetadata thisMetadata;
	private final Lineage thisLineage;
	
	public ThisScope(DataSet thisValue, Lineage thisLineage)
	{
		this.thisValue = thisValue;
		this.thisMetadata = thisValue.getMetadata();
		this.thisLineage = thisLineage;
	}

	public ThisScope(DataSetMetadata thisMetadata, Lineage thisLineage)
	{
		this.thisValue = null;
		this.thisMetadata = thisMetadata;
		this.thisLineage = thisLineage;
	}

	@Override
	public boolean contains(String alias)
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public VTLValueMetadata getMetadata(String alias)
	{
		if (THIS.equals(alias))
			return thisMetadata;
		else
		{
			final String normalizedAlias = normalizeAlias(alias);
			if (thisMetadata instanceof DataSetMetadata && ((DataSetMetadata) thisMetadata).getComponent(normalizedAlias).isPresent())
				return ((DataSetMetadata) thisMetadata).membership(normalizedAlias);
			else 
				throw new VTLMissingComponentsException(alias, (DataSetMetadata) thisMetadata);
		}
	}

	@Override
	public VTLValue resolve(String alias)
	{
		if (THIS.equals(alias))
			return thisValue;
		else 
		{
			final String normalizedAlias = DataStructureComponent.normalizeAlias(alias);
			if (thisValue instanceof DataSet && ((DataSet) thisValue).getComponent(normalizedAlias).isPresent())
				return ((DataSet) thisValue).membership(normalizedAlias, thisLineage);
			else 
				throw new VTLMissingComponentsException(normalizedAlias, thisMetadata);
		}
	}

	@Override
	public Statement getRule(String node)
	{
		throw new VTLUnboundAliasException(node);
	}

	@Override
	public MetadataRepository getRepository()
	{
		return ConfigurationManager.getDefault().getMetadataRepository();
	}

	@Override
	public Optional<Lineage> linkLineage(String alias)
	{
		throw new UnsupportedOperationException();
	}
}
