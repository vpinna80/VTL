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
import java.util.function.Function;

public class Quadruple<A extends Serializable, B extends Serializable, C extends Serializable, D extends Serializable> extends Triple<A, B, C>
{
	private static final long serialVersionUID = 1L;
	
	protected final D d;

	public Quadruple(A a, B b, C c, D d)
	{
		super(a, b, c);

		this.d = d;
	}

	public Quadruple(Triple<? extends A, ? extends B, ? extends C> t, D d)
	{
		super(t);

		this.d = d;
	}

	public D fourth()
	{
		return d;
	}	

	public <D2 extends Serializable> Quadruple<A, B, C, D2> map4(Function<? super D, ? extends D2> mapper)
	{
		return new Quadruple<>(a, b, c, mapper.apply(d));
	}
}
