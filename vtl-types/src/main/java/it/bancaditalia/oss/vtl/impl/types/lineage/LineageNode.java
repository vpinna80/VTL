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
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class LineageNode extends LineageImpl
{
	private static final long serialVersionUID = 1L;
	private static final Map<Entry<String, LineageSet>, SoftReference<LineageNode>> CACHE2 = new ConcurrentHashMap<>();
	private static final Logger LOGGER = LoggerFactory.getLogger(LineageNode.class);

	private final String transformation;
	private final LineageSet sources;

	public static LineageNode of(String transformation, Lineage... sources)
	{
		if (sources.length == 1 && sources[0] instanceof LineageCall)
			return of(transformation, (LineageCall) sources[0]);
		else
			return of(transformation, LineageCall.of(sources));
	}
	
	public static SerUnaryOperator<Lineage> lineageEnricher(Transformation transformation)
	{
		return lineage -> LineageNode.of(transformation, lineage);
	}
	
	public static LineageNode of(Transformation transformation, LineageSet sources)
	{
		return of(transformation.toString(), sources);
	}

	public static LineageNode of(Transformation transformation, Lineage... sources)
	{
		return of(transformation.toString(), sources);
	}

	private static LineageNode of(String transformation, LineageSet sources)
	{
		Entry<String, LineageSet> entry = new SimpleEntry<>(transformation, sources);
		LineageNode instance = CACHE2.computeIfAbsent(entry, e -> {
			LOGGER.trace("Creating lineage for {} with {}...", transformation, sources);
			final SoftReference<LineageNode> lineage = new SoftReference<>(new LineageNode(transformation, sources));
			LOGGER.trace("Lineage created for {}.", transformation);
			return lineage;
		}).get();

		// Small chance that the reference if gced while being resolved
		if (instance == null)
		{
			instance = new LineageNode(transformation, sources);
			CACHE2.put(entry, new SoftReference<>(instance));
		}
		return instance;
	}

	private LineageNode(String transformation, LineageSet sources)
	{
		Objects.requireNonNull(transformation);
		Objects.requireNonNull(sources);

		this.transformation = transformation.toString();
		this.sources = sources;
	}

	public LineageSet getSourceSet()
	{
		return sources;
	}

	@Override
	public String toString()
	{
		return getTransformation().toString();
	}

	@Override
	public Lineage resolveExternal(TransformationScheme scheme)
	{
		return of(getTransformation(), sources.resolveExternal(scheme));
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sources == null) ? 0 : sources.hashCode());
		result = prime * result + ((getTransformation() == null) ? 0 : getTransformation().hashCode());
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
		if (getTransformation() == null)
		{
			if (other.getTransformation() != null)
				return false;
		} else if (!getTransformation().equals(other.getTransformation()))
			return false;
		return true;
	}

	public String getTransformation()
	{
		return transformation;
	}
}
