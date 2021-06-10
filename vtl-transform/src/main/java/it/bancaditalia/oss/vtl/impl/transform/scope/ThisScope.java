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

import java.util.Optional;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class ThisScope implements TransformationScheme
{
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
	public VTLValueMetadata getMetadata(String node)
	{
		if (THIS.equals(node))
			return thisMetadata;
		else
		{
			final String stripped = node.matches("'.*'") ? node.replaceAll("'(.*)'", "$1") : node.toLowerCase();
			if (thisMetadata instanceof DataSetMetadata && ((DataSetMetadata) thisMetadata).getComponent(stripped).isPresent())
				return ((DataSetMetadata) thisMetadata).membership(stripped);
			else 
				throw new VTLMissingComponentsException(stripped, (DataSetMetadata) thisMetadata);
		}
	}

	@Override
	public VTLValue resolve(String node)
	{
		if (THIS.equals(node))
			return thisValue;
		else 
		{
			final String stripped = node.matches("'.*'") ? node.replaceAll("'(.*)'", "$1") : node.toLowerCase();
			if (thisValue instanceof DataSet && ((DataSet) thisValue).getComponent(node).isPresent())
				return ((DataSet) thisValue).membership(stripped, thisLineage);
			else 
				throw new VTLMissingComponentsException(stripped, thisMetadata);
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
