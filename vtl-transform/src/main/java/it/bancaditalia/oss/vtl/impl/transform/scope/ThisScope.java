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

import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;

import java.util.Optional;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class ThisScope extends AbstractScope
{
	private static final long serialVersionUID = 1L;
	public static final VTLAlias THIS = VTLAliasImpl.of(true, "$$THIS");
	
	private final DataSet thisValue;
	private final DataSetStructure thisMetadata;
	private final TransformationScheme parentScheme;
	
	public ThisScope(TransformationScheme parentScheme, DataSet thisValue)
	{
		this.thisValue = thisValue;
		this.thisMetadata = thisValue.getMetadata();
		this.parentScheme = parentScheme;
	}

	public ThisScope(TransformationScheme parentScheme, DataSetStructure thisMetadata)
	{
		this.thisValue = null;
		this.thisMetadata = thisMetadata;
		this.parentScheme = parentScheme;
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
				throw new VTLMissingComponentsException((DataSetStructure) thisMetadata, alias);
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
				return thisValue.membership(alias, identity());
			else 
				throw new VTLMissingComponentsException(thisMetadata, alias);
		}
	}

	@Override
	public Optional<Statement> getRule(VTLAlias node)
	{
		return parentScheme.getRule(node);
	}

	@Override
	public MetadataRepository getRepository()
	{
		return parentScheme.getRepository();
	}
}
