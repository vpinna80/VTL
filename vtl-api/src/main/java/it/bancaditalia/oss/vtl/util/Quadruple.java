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
import java.util.function.Function;

public class Quadruple<A extends Serializable, B extends Serializable, C extends Serializable, D extends Serializable> implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final A first;
	private final B second;
	private final C third;
	private final D fourth;

	public Quadruple(A a, B b, C c, D d)
	{
		this.first = a;
		this.second = b;
		this.third = c;
		this.fourth = d;
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

	public D getFourth()
	{
		return fourth;
	}	

	public <B2 extends Serializable> Quadruple<A, B2, C, D> map2(Function<? super Quadruple<A, B, C, D>, ? extends B2> mapper)
	{
		return new Quadruple<>(first, mapper.apply(this), third, fourth);
	}
	
	public <C2 extends Serializable> Quadruple<A, B, C2, D> map3(Function<? super Quadruple<A, B, C, D>, ? extends C2> mapper)
	{
		return new Quadruple<>(first, second, mapper.apply(this), fourth);
	}

	public <D2 extends Serializable> Quadruple<A, B, C, D2> map4(Function<? super Quadruple<A, B, C, D>, ? extends D2> mapper)
	{
		return new Quadruple<>(first, second, third, mapper.apply(this));
	}
}
