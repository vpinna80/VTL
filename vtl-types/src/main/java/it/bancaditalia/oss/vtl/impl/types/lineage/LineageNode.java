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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class LineageNode implements Lineage
{
	private final static Map<Entry<Transformation, List<Lineage>>, SoftReference<LineageNode>> CACHE = new ConcurrentHashMap<>();
	private final static Logger LOGGER = LoggerFactory.getLogger(LineageNode.class);

	private final Transformation transformation;
	private final LineageChain sources;

	public static LineageNode of(Transformation transformation, Lineage... sources)
	{
		return of(transformation, Arrays.asList(sources));
	}
	
	public static LineageNode of(Transformation transformation, List<Lineage> sources)
	{
		Entry<Transformation, List<Lineage>> entry = new SimpleEntry<>(transformation, sources);
		LineageNode instance = CACHE.computeIfAbsent(entry, e -> {
				LOGGER.trace("Creating lineage for {} with {}...", transformation, sources);
				final SoftReference<LineageNode> lineage = new SoftReference<>(new LineageNode(transformation, sources));
				LOGGER.debug("Lineage created for {}.", transformation);
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

	private LineageNode(Transformation transformation, List<Lineage> sources)
	{
		Objects.requireNonNull(transformation);
		Objects.requireNonNull(sources);
		
		this.transformation = transformation;
		if (sources.size() == 1 && sources.get(0) instanceof LineageChain)
			this.sources = (LineageChain) sources.get(0);
		else
			this.sources = LineageChain.of(sources);
		
		sources.stream().forEach(Objects::requireNonNull);
	}

	public Transformation getGenerator()
	{
		return transformation;
	}

	public List<Lineage> getSources()
	{
		return sources.getSources();
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
}
