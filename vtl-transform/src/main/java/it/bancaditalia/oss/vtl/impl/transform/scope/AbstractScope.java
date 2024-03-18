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

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public abstract class AbstractScope implements TransformationScheme, Serializable
{
	private static final long serialVersionUID = 1L;

	private final Map<Class<?>, Map<Transformation, ?>> holders = new ConcurrentHashMap<>();

	@Override
	public <T> Map<Transformation, T> getResultHolder(Class<T> type)
	{
		Map<Transformation, ?> holder = holders.get(type);
		if (holder == null)
			holder = holders.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
		
		@SuppressWarnings("unchecked")
		Map<Transformation, T> result = (Map<Transformation, T>) holder;
		return result;
	}
	
	@Override
	public DataPointRuleSet findDatapointRuleset(String alias)
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public HierarchicalRuleSet<?, ?, ?> findHierarchicalRuleset(String alias)
	{
		throw new UnsupportedOperationException();
	}
}
