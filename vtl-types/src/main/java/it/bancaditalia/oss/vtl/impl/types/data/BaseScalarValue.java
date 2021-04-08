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

import java.io.Serializable;
import java.util.Objects;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

/**
 * @author Valentino Pinna
 *
 * @param <V> The concrete implementation type
 * @param <R> The type of the value wrapped by the implementation
 * @param <S> The domain subset
 * @param <D> the parent domain
 */
public abstract class BaseScalarValue<V extends BaseScalarValue<V, R, S, D>, R extends Comparable<?> & Serializable, S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements ScalarValue<V, R, S, D>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final R		value;
	private final S		domain;
	private final int	hashCode;

	public BaseScalarValue(R value, S domain)
	{
		this.value = value;
		this.domain = Objects.requireNonNull(domain, "Domain cannot be null");
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
		return this::getDomain;
	}

	@Override
	public String toString()
	{
		return value.toString();
	}

	@Override
	public final int hashCode()
	{
		return hashCode;
	}
	
	@Override
	public final boolean equals(Object obj)
	{
		if (obj == null)
			return false;
		if (!(obj instanceof BaseScalarValue))
			return false;
		
		BaseScalarValue<?, ?, ?, ?> other = (BaseScalarValue<?, ?, ?, ?>) obj;
		if (value == null)
			return other.value == null;
		if (value.equals(other.value))
			return true;
		if (domain.isAssignableFrom(other.getDomain()))
			return value.equals(domain.cast(other).get());
		else if (other.getDomain().isAssignableFrom(domain))
			return other.domain.cast(this).equals(other.value);
		
		return false;
	}
}
