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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

class ScalarParameter extends BaseParameter
{
	private static final long serialVersionUID = 1L;
	
	protected final String domainName;

	public ScalarParameter(String name, String domainName)
	{
		super(name);
		this.domainName = domainName;
	}

	public String getDomain()
	{
		return domainName;
	}
	
	@Override
	public String toString()
	{
		return getAlias() + (domainName != null ? " " + domainName : "");
	}

	@Override
	public boolean matches(TransformationScheme scheme, Transformation argument)
	{
		VTLValueMetadata metadata = argument.getMetadata(scheme);
		return metadata instanceof ScalarValueMetadata || metadata instanceof UnknownValueMetadata;
	}
	
	@Override
	public String getDefinitionString()
	{
		return domainName;
	}
}
