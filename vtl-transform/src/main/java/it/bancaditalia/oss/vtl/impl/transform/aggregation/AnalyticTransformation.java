package it.bancaditalia.oss.vtl.impl.transform.aggregation;

import java.io.Serializable;

public interface AnalyticTransformation
{
	public enum OrderingMethod 
	{
		ASC, DESC
	};

	public static class OrderByItem implements Serializable
	{
		private static final long serialVersionUID = 1L;
		private final String name;
		private final OrderingMethod method;

		public OrderByItem(String name, OrderingMethod method)
		{
			this.name = name;
			this.method = method;
		}

		public String getName()
		{
			return name;
		}

		public OrderingMethod getMethod()
		{
			return method;
		}
		
		@Override
		public String toString()
		{
			return (method != null ? method + " " : "") + name;
		}
	}
}
