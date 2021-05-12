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

import static java.util.stream.Collectors.toList;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class LineageCall extends LineageImpl implements LineageSet
{
	private final static Map<List<Lineage>, SoftReference<LineageCall>> CACHE = new ConcurrentHashMap<>();
	private final List<Lineage> sources;

	public static LineageCall of(Lineage... sources)
	{
		return of(Arrays.asList(sources));
	}

	public static LineageCall of(List<Lineage> sources)
	{
		sources.forEach(e -> {
			if (e instanceof LineageSet)
				throw new NullPointerException();
		});
		
		LineageCall instance = CACHE.computeIfAbsent(sources, s -> new SoftReference<>(new LineageCall(s))).get();
		if (instance == null)
		{
			instance = new LineageCall(sources);
			CACHE.put(sources, new SoftReference<>(instance));
		}
		return instance;
	}

	private LineageCall(List<Lineage> sources)
	{
		this.sources = sources;
	}

	@Override
	public LineageCall resolveExternal(TransformationScheme scheme)
	{
		return LineageCall.of(sources.stream().map(l -> l.resolveExternal(scheme)).collect(toList()));
	}

	public List<Lineage> getSources()
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
		LineageCall other = (LineageCall) obj;
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
		return sources.size();
	}
}
