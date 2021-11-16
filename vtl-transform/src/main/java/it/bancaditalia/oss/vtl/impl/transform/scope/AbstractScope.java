package it.bancaditalia.oss.vtl.impl.transform.scope;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
}
