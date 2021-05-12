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
package it.bancaditalia.oss.vtl.impl.types.lineage;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class LineageExternal extends LineageImpl
{
	private final static Map<String, SoftReference<LineageExternal>> CACHE = new ConcurrentHashMap<>();
	
	private final String source;

	public static LineageExternal of(String externalDescription)
	{
		LineageExternal instance = CACHE.computeIfAbsent(externalDescription, d -> new SoftReference<>(new LineageExternal(d))).get();
		if (instance == null)
		{
			instance = new LineageExternal(externalDescription);
			CACHE.put(externalDescription, new SoftReference<>(instance));
		}
		return instance;
	}
	
	private LineageExternal(String externalDescription)
	{
		this.source = externalDescription;
	}
	
	@Override
	public String toString()
	{
		return source;
	}
	
	@Override
	public Lineage resolveExternal(TransformationScheme scheme)
	{
		return scheme.linkLineage(source);
	}
}
