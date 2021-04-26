package it.bancaditalia.oss.vtl.impl.transform.util;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class MetadataHolder extends ConcurrentHashMap<Transformation, VTLValueMetadata>
{
	private static final long serialVersionUID = 1L;
	private static final Map<TransformationScheme, MetadataHolder> HOLDERS = new WeakHashMap<>(); 

	private MetadataHolder()
	{
		super();
	}
	
	public static MetadataHolder getInstance(TransformationScheme scheme)
	{
		MetadataHolder holder = HOLDERS.get(scheme);
		if (holder == null)
			synchronized (HOLDERS)
			{
				holder = new MetadataHolder();
				HOLDERS.put(scheme, holder);
			}

		return holder;
	}
}
