/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.util;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.function.Function;

public class Triple<A extends Serializable, B extends Serializable, C extends Serializable> implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	protected final A a;
	protected final B b;
	protected final C c;
	
	public Triple(A a, B b, C c)
	{
		this.a = a;
		this.b = b;
		this.c = c;
	}

	public Triple(Triple<? extends A, ? extends B, ? extends C> t)
	{
		this(t.a, t.b, t.c);
	}

	public Triple(A a, Entry<? extends B, ? extends C> e)
	{
		this(a, e.getKey(), e.getValue());
	}

	public Triple(Entry<? extends A, ? extends B> e, C c)
	{
		this(e.getKey(), e.getValue(), c);
	}
	
	public <D extends Serializable> Quadruple<A, B, C, D> extend(D d)
	{
		return new Quadruple<>(this, d);
	}
	
	public A first()
	{
		return a;
	}

	public B second()
	{
		return b;
	}

	public C third()
	{
		return c;
	}
	
	public <B2 extends Serializable> Triple<A, B2, C> map2(Function<? super Triple<A, B, C>, ? extends B2> mapper)
	{
		return new Triple<>(a, mapper.apply(this), c);
	}
	
	public <C2 extends Serializable> Triple<A, B, C2> map3(Function<? super Triple<A, B, C>, ? extends C2> mapper)
	{
		return new Triple<>(a, b, mapper.apply(this));
	}
}