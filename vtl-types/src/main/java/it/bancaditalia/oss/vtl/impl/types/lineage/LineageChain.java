package it.bancaditalia.oss.vtl.impl.types.lineage;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class LineageChain implements Lineage
{
	private final static Map<List<Lineage>, SoftReference<LineageChain>> CACHE = new ConcurrentHashMap<>();
	private final List<Lineage> sources;

	public static LineageChain of(Lineage... sources)
	{
		return of(Arrays.asList(sources));
	}

	public static LineageChain of(List<Lineage> sources)
	{
		LineageChain instance = CACHE.computeIfAbsent(sources, s -> new SoftReference<>(new LineageChain(s))).get();
		if (instance == null)
		{
			instance = new LineageChain(sources);
			CACHE.put(sources, new SoftReference<>(instance));
		}
		return instance;
	}

	public static LineageChain of(Set<Lineage> sources)
	{
		return of(new ArrayList<>(sources));
	}
	
	private LineageChain(List<Lineage> sources)
	{
		this.sources = sources;
	}
	
	@Override
	public Lineage resolveExternal(TransformationScheme scheme)
	{
		List<Lineage> resolvedSources = new ArrayList<>((getSources().size()));
		for (Lineage source: getSources())
			resolvedSources.add(source.resolveExternal(scheme));
		
		return LineageChain.of(resolvedSources);
	}

	public List<Lineage> getSources()
	{
		return sources;
	}
}
