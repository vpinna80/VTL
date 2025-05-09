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
package it.bancaditalia.oss.vtl.impl.types.data;

import static it.bancaditalia.oss.vtl.util.Utils.EPSILON;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Objects;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

/**
 * @author Valentino Pinna
 *
 * @param <V> The concrete implementation type
 * @param <R> The type of the value wrapped by the implementation
 * @param <S> The domain subset
 * @param <D> the parent domain
 */
public abstract class BaseScalarValue<V extends BaseScalarValue<V, R, S, D>, R extends Comparable<? super R> & Serializable, S extends ValueDomainSubset<S, D>, D extends ValueDomain> 
	implements ScalarValue<V, R, S, D>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final R		value;
	private final S		domain;
	private final int	hashCode;

	public BaseScalarValue(R value, S domain)
	{
		this.value = value;
		this.domain = requireNonNull(domain, "Domain cannot be null");
		this.hashCode = 31 + (value == null ? 0 : value.hashCode());
	}

	@Override
	public final R get()
	{
		return value;
	}

	@Override
	public final S getDomain()
	{
		return domain;
	}
	
	@Override
	public final ScalarValueMetadata<S, D> getMetadata()
	{
		return new ScalarValueMetadata<S, D>() 
		{
			private static final long serialVersionUID = 1L;

			@Override
			public S getDomain()
			{
				return domain;
			}
			
			@Override
			public String toString()
			{
				return "scalar<" + domain.toString() + ">";
			}
		};
	}

	@Override
	public String toString()
	{
		return Objects.toString(value);
	}

	@Override
	public final int hashCode()
	{
		return hashCode;
	}
	
	@Override
	public final boolean equals(Object obj)
	{
		if (obj == null || !(obj instanceof BaseScalarValue))
			return false;
		
		BaseScalarValue<?, ?, ?, ?> other = (BaseScalarValue<?, ?, ?, ?>) obj;
		if (value == null)
			return other.value == null;
		else if (other.value == null)
			return false;
		else if (value.equals(other.value))
			return true;
		else if (value.getClass() == Double.class && other.value.getClass() == Double.class)
		{
			double d1 = (Double) get(), d2 = (Double) other.get();
			double tol = (2 << (int) (3.33 * EPSILON)) * max(1.0, max(abs(d1), abs(d2)));

			return abs(d1 - d2) < tol;
		}
		else if (domain.isAssignableFrom(other.getDomain()))
			return equals(domain.cast(other).get());
		else if (other.getDomain().isAssignableFrom(domain))
			return other.domain.cast(this).equals(other);
		
		return false;
	}
	
	@Override
	public boolean isNull()
	{
		return false;
	}
}
