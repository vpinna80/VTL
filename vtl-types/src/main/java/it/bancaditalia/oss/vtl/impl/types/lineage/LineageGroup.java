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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class LineageGroup extends LineageImpl implements LineageSet
{
	private final static Map<Map<Lineage, Long>, SoftReference<LineageGroup>> CACHE = new ConcurrentHashMap<>();
	private final Map<Lineage, Long> sources;

//	public static LineageGroup of(Lineage... sources)
//	{
//		Map<Lineage, Long> sourcesMap = Arrays.stream(sources).collect(toMapWithValues(k -> 1L));
//		return of(sourcesMap);
//	}
//
//	public static LineageGroup of(Collection<Lineage> sources)
//	{
//		Map<Lineage, Long> sourcesMap = sources.stream().collect(toMapWithValues(k -> 1L));
//		return of(sourcesMap);
//	}
//
	public static LineageGroup of(Map<Lineage, Long> sources)
	{
		sources.keySet().forEach(e -> {
			if (e instanceof LineageGroup)
				throw new NullPointerException();
		});
		
		LineageGroup instance = CACHE.computeIfAbsent(sources, s -> new SoftReference<>(new LineageGroup(s))).get();
		if (instance == null)
		{
			instance = new LineageGroup(sources);
			CACHE.put(sources, new SoftReference<>(instance));
		}
		return instance;
	}

	private LineageGroup(Map<Lineage, Long> sources)
	{
		this.sources = sources;
	}

	@Override
	public LineageGroup resolveExternal(TransformationScheme scheme)
	{
		Map<Lineage, Long> resolvedSources = new HashMap<>((getSources().size()));
		for (Entry<Lineage, Long> source : getSources().entrySet())
			resolvedSources.put(source.getKey().resolveExternal(scheme), source.getValue());

		return LineageGroup.of(resolvedSources);
	}

	public Map<Lineage, Long> getSources()
	{
		return sources;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sources == null) ? 0 : sources.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LineageGroup other = (LineageGroup) obj;
		if (sources == null)
		{
			if (other.sources != null)
				return false;
		}
		else if (!sources.equals(other.sources))
			return false;
		return true;
	}

	public long size()
	{
		return sources.values().parallelStream().mapToLong(Long::longValue).sum();
	}
}
