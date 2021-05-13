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
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class LineageNode extends LineageImpl
{
	private final static Map<Entry<Transformation, LineageSet>, SoftReference<LineageNode>> CACHE = new ConcurrentHashMap<>();
	private final static Logger LOGGER = LoggerFactory.getLogger(LineageNode.class);

	private final Transformation transformation;
	private final LineageSet sources;

	public static LineageNode of(Transformation transformation, Lineage... sources)
	{
		if (sources.length == 1 && sources[0] instanceof LineageCall)
			return of(transformation, (LineageCall) sources[0]);
		else if (sources.length == 1 && sources[0] instanceof LineageGroup)
			return of(transformation, (LineageGroup) sources[0]);
		else
			return of(transformation, LineageCall.of(sources));
	}
	
	public static LineageNode of(Transformation transformation, LineageSet sources)
	{
		Entry<Transformation, LineageSet> entry = new SimpleEntry<>(transformation, sources);
		LineageNode instance = CACHE.computeIfAbsent(entry, e -> {
			LOGGER.trace("Creating lineage for {} with {}...", transformation, sources);
			final SoftReference<LineageNode> lineage = new SoftReference<>(new LineageNode(transformation, sources));
			LOGGER.trace("Lineage created for {}.", transformation);
			return lineage;
		}).get();

		// Small chance that the reference if gced while being resolved
		if (instance == null)
		{
			instance = new LineageNode(transformation, sources);
			CACHE.put(entry, new SoftReference<>(instance));
		}
		return instance;
	}

	private LineageNode(Transformation transformation, LineageSet sources)
	{
		Objects.requireNonNull(transformation);
		Objects.requireNonNull(sources);

		this.transformation = transformation;
		this.sources = sources;
	}

	public LineageSet getSourceSet()
	{
		return sources;
	}

	@Override
	public String toString()
	{
		return transformation.toString();
	}

	@Override
	public Lineage resolveExternal(TransformationScheme scheme)
	{
		return LineageNode.of(transformation, sources.resolveExternal(scheme));
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sources == null) ? 0 : sources.hashCode());
		result = prime * result + ((transformation == null) ? 0 : transformation.hashCode());
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
		LineageNode other = (LineageNode) obj;
		if (sources == null)
		{
			if (other.sources != null)
				return false;
		} else if (!sources.equals(other.sources))
			return false;
		if (transformation == null)
		{
			if (other.transformation != null)
				return false;
		} else if (!transformation.equals(other.transformation))
			return false;
		return true;
	}

	public Transformation getTransformation()
	{
		return transformation;
	}
}
