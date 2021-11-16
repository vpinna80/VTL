package it.bancaditalia.oss.vtl.util;

import java.util.Map.Entry;

public class IntValuedEntry<K> implements Entry<K, Integer>
{
	private final K key;
	private final int value;

	public IntValuedEntry(K key, int value)
	{
		this.key = key;
		this.value = value;
	}

	@Override
	public K getKey()
	{
		return key;
	}

	@Override
	public Integer getValue()
	{
		return value;
	}

	public int getIntValue()
	{
		return value;
	}

	@Override
	public Integer setValue(Integer value)
	{
		throw new UnsupportedOperationException();
	}
}
