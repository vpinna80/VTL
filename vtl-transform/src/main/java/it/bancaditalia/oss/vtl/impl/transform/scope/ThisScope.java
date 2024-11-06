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

import it.bancaditalia.oss.vtl.engine.DMLStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class ThisScope extends AbstractScope
{
	private static final long serialVersionUID = 1L;
	public static final VTLAlias THIS = VTLAliasImpl.of("$$THIS");
	
	private final DataSet thisValue;
	private final DataSetMetadata thisMetadata;
	private final MetadataRepository repo;
	
	public ThisScope(MetadataRepository repo, DataSet thisValue)
	{
		this.repo = repo;
		this.thisValue = thisValue;
		this.thisMetadata = thisValue.getMetadata();
	}

	public ThisScope(MetadataRepository repo, DataSetMetadata thisMetadata)
	{
		this.repo = repo;
		this.thisValue = null;
		this.thisMetadata = thisMetadata;
	}

	@Override
	public boolean contains(VTLAlias alias)
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public VTLValueMetadata getMetadata(VTLAlias alias)
	{
		if (THIS.equals(alias))
			return thisMetadata;
		else
		{
			if (thisMetadata.getComponent(alias).isPresent())
				return thisMetadata.membership(alias);
			else 
				throw new VTLMissingComponentsException(alias, (DataSetMetadata) thisMetadata);
		}
	}

	@Override
	public VTLValue resolve(VTLAlias alias)
	{
		if (THIS.equals(alias))
			return thisValue;
		else 
		{
			if (thisValue.getComponent(alias).isPresent())
				return thisValue.membership(alias);
			else 
				throw new VTLMissingComponentsException(alias, thisMetadata);
		}
	}

	@Override
	public DMLStatement getRule(VTLAlias node)
	{
		throw new VTLUnboundAliasException(node);
	}

	@Override
	public MetadataRepository getRepository()
	{
		return repo;
	}
}
