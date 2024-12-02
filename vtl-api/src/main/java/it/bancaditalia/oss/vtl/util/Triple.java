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
package it.bancaditalia.oss.vtl.util;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.function.Function;

public class Triple<A, B, C> implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final A first;
	private final B second;
	private final C third;
	
	public Triple(A a, B b, C c)
	{
		this.first = a;
		this.second = b;
		this.third = c;
	}

	public Triple(Triple<? extends A, ? extends B, ? extends C> t)
	{
		this(t.first, t.second, t.third);
	}

	public Triple(A a, Entry<? extends B, ? extends C> e)
	{
		this(a, e.getKey(), e.getValue());
	}

	public Triple(Entry<? extends A, ? extends B> e, C c)
	{
		this(e.getKey(), e.getValue(), c);
	}
	
	public A getFirst()
	{
		return first;
	}

	public B getSecond()
	{
		return second;
	}

	public C getThird()
	{
		return third;
	}
	
	public <B2 extends Serializable> Triple<A, B2, C> map2(Function<? super Triple<A, B, C>, ? extends B2> mapper)
	{
		return new Triple<>(first, mapper.apply(this), third);
	}
	
	public <C2 extends Serializable> Triple<A, B, C2> map3(Function<? super Triple<A, B, C>, ? extends C2> mapper)
	{
		return new Triple<>(first, second, mapper.apply(this));
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		result = prime * result + ((third == null) ? 0 : third.hashCode());
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
		Triple<?, ?, ?> other = (Triple<?, ?, ?>) obj;
		if (first == null)
		{
			if (other.first != null)
				return false;
		}
		else if (!first.equals(other.first))
			return false;
		if (second == null)
		{
			if (other.second != null)
				return false;
		}
		else if (!second.equals(other.second))
			return false;
		if (third == null)
		{
			if (other.third != null)
				return false;
		}
		else if (!third.equals(other.third))
			return false;
		return true;
	}
	
	@Override
	public String toString()
	{
		return "<" + first + ", " + second + ", " + third + ">";
	}
}