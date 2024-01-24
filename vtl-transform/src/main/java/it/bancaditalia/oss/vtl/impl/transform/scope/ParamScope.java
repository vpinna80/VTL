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

import static java.util.stream.Collectors.toMap;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class ParamScope extends AbstractScope
{
	private static final long serialVersionUID = 1L;
	
	private final TransformationScheme parent;
	private final Map<String, Transformation> params;

	private final Map<String, VTLValueMetadata> parametersMeta;

	public ParamScope(TransformationScheme parent, Map<String, Transformation> params)
	{
		this.parent = parent;
		this.params = params;

		parametersMeta = params.entrySet().stream()
				.collect(toMap(Entry::getKey, entry -> entry.getValue().getMetadata(parent)));
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
		if (parametersMeta.containsKey(alias))
			return parametersMeta.get(alias);
		else
			return Utils.getStream(parametersMeta.values())
				.filter(DataSetMetadata.class::isInstance)
				.map(DataSetMetadata.class::cast)
				.map(dataset -> dataset.getComponent(alias))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.map(DataStructureComponent::getVariable)
				.map(VTLValueMetadata.class::cast)
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
		return parametersMeta.containsKey(alias);
	}
}
