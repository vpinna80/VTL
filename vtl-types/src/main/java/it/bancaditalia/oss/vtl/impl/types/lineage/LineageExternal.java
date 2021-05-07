package it.bancaditalia.oss.vtl.impl.types.lineage;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class LineageExternal implements Lineage
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
