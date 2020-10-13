/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.transform.scope;

import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;

import java.util.Map;
import java.util.Optional;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class ParamScope implements TransformationScheme
{
	private final TransformationScheme parent;
	private final Map<String, Transformation> params;

	private volatile transient Map<String, VTLValueMetadata> parametersMeta;

	public ParamScope(TransformationScheme parent, Map<String, Transformation> params)
	{
		this.parent = parent;
		this.params = params;
	}

	@Override
	public VTLValue resolve(String node)
	{
		if (params.containsKey(node))
			return params.get(node).eval(getParent());
		else
			return getParent().resolve(node);
	}

	@Override
	public VTLValueMetadata getMetadata(String alias)
	{
		initParams();

		if (parametersMeta.containsKey(alias))
			return parametersMeta.get(alias);
		else
			return Utils.getStream(parametersMeta.values())
				.filter(DataSetMetadata.class::isInstance)
				.map(DataSetMetadata.class::cast)
				.map(dataset -> dataset.getComponent(alias))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.map(component -> (VTLValueMetadata) (ScalarValueMetadata<?>) component::getDomain)
				.findAny()
				.orElseGet(() -> getParent().getMetadata(alias));
	}

	@Override
	public Statement getRule(String node)
	{
		return getParent().getRule(node);
	}

	@Override
	public MetadataRepository getRepository()
	{
		return getParent().getRepository();
	}

	public TransformationScheme getParent()
	{
		return parent;
	}

	@Override
	public boolean contains(String alias)
	{
		initParams();

		return parametersMeta.containsKey(alias);
	}

	private synchronized void initParams()
	{
		if (parametersMeta == null)
			parametersMeta = Utils.getStream(params)
				.map(keepingKey(transformation -> transformation.getMetadata(parent)))
				.collect(entriesToMap());
	}
}
