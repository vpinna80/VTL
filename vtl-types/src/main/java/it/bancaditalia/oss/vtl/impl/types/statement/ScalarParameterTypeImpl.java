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
package it.bancaditalia.oss.vtl.impl.types.statement;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.ScalarParameterType;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ScalarParameterTypeImpl implements ScalarParameterType, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias domainName;
	private final ScalarValue<?, ?, ?, ?> defaultValue;

	public ScalarParameterTypeImpl(VTLAlias domainName, ScalarValue<?, ?, ?, ?> defaultValue)
	{
		this.domainName = domainName;
		this.defaultValue = defaultValue;
	}

	@Override
	public String toString()
	{
		return getDomainName().toString();
	}

	@Override
	public boolean matches(TransformationScheme scheme, Transformation argument)
	{
		VTLValueMetadata metadata = argument.getMetadata(scheme);
		return !metadata.isDataSet() || metadata instanceof UnknownValueMetadata;
	}
	
	@Override
	public ScalarValue<?, ?, ?, ?> getDefaultValue()
	{
		return defaultValue;
	}

	@Override
	public VTLAlias getDomainName()
	{
		return domainName;
	}
}
